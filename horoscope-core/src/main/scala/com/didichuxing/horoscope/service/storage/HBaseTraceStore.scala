/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.storage

import java.util
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import com.alibaba.ttl.threadpool.TtlExecutors
import com.didichuxing.horoscope.core.FlowRuntimeMessage._
import com.didichuxing.horoscope.core.{FlowRuntimeMessage, TraceStore}
import com.didichuxing.horoscope.service.source.NamedThreadFactory
import com.didichuxing.horoscope.service.storage.HBaseTraceStore._
import com.didichuxing.horoscope.util.Constants._
import com.didichuxing.horoscope.util.Utils._
import com.didichuxing.horoscope.util.{Logging, Utils}
import com.google.common.hash.Hashing
import com.google.protobuf.Empty
import com.google.protobuf.util.JsonFormat
import com.typesafe.config.Config
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, HColumnDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Try

/**
 * traceContext
 * ------------------------------------------------------------------------------------------------
 * |      rowkey     |            mailbox(cf)             |  scheduler(cf)      |  context(cf)    |
 * ------------------------------------------------------------------------------------------------
 * |   slot_traceId  | source1_eventId | source2_eventId  |   _SCD__eventId      |  context(array) |
 * ------------------------------------------------------------------------------------------------
 *
 * schedulerToken
 * ---------------------------
 * |   rowkey    | owner(cf) |
 * ---------------------------
 * |  lockInfo   |  owner    |
 * ---------------------------
 *
 * schedulerSource
 * ----------------------------------------------------------------
 * |          rowkey           |              ss(cf)               |
 * ----------------------------------------------------------------
 * |  slot_timestamp_eventId   |   source1       |      source2    |
 * ----------------------------------------------------------------
 *
 */
class HBaseTraceStore(executionContext: ExecutionContext = null) extends AbstractTraceStore with Logging {

  private var conn: Connection = _
  private var traceContextTableName: TableName = _
  private var schedulerTableName: TableName = _
  private var tokenTableName: TableName = _
  private var slotCount = 0
  implicit val ec = if (executionContext == null) {
    val pool = new ThreadPoolExecutor(2, 20, 1000L, TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue[Runnable](1000), new NamedThreadFactory("hbaseTraceStore"))
    ExecutionContext.fromExecutorService(TtlExecutors.getTtlExecutorService(pool))
  } else {
    executionContext
  }

  override def start(conf: Config): Unit = {
    super.start(conf)
    val hbaseConfig = HBaseConfiguration.create()
    hbaseConfig.set("hbase.zookeeper.quorum", config.getString("horoscope.hbase.zookeeper.quorum"))
    hbaseConfig.set("hbase.zookeeper.property.clientPort", config.getString("horoscope.hbase.zookeeper.port"))
    hbaseConfig.set("hbase.user.name", config.getString("horoscope.hbase.user.name"))
    hbaseConfig.set("hbase.user.password", config.getString("horoscope.hbase.user.password"))
    conn = ConnectionFactory.createConnection(hbaseConfig)
    traceContextTableName = TableName.valueOf(config.getString("horoscope.hbase.trace-context-table"))
    schedulerTableName = TableName.valueOf(config.getString("horoscope.hbase.scheduler-source-table"))
    tokenTableName = TableName.valueOf(config.getString("horoscope.hbase.token-table"))
    slotCount = Utils.getClusterSlotCount(config)
    debug(("msg", "start hbase trace store"))
  }

  override def stop(): Unit = {
    close(conn)
    debug(("msg", "stop hbase trace store"))
  }

  /**
   * Add a new event to store, try to acquire and return token when needed
   */
  override def addEvent(source: String, event: FlowEvent.Builder): FlowRuntimeMessage.FlowEventOrBuilder = {
    val traceContextTable = conn.getTable(traceContextTableName)
    try {
      val traceId = event.getTraceId
      val rowKey = getRowKey(traceId)
      val flowEvent = event.build()
      val p = new Put(Bytes.toBytes(rowKey))
      val sourceCol = getSourceCol(source, event.getEventId)
      p.addColumn(getSourceColumnFamily(source), sourceCol, flowEvent.toByteArray)
      traceContextTable.put(p)
      flowEvent
    } finally {
      close(traceContextTable)
    }
  }

  override def addEvents(source: String, events: List[FlowEvent.Builder]): List[FlowEventOrBuilder] = {
    val traceContextTable = conn.getTable(traceContextTableName)
    try {
      val puts = new util.ArrayList[Put](events.size)
      for (event <- events) {
        val traceId = event.getTraceId
        val rowKey = getRowKey(traceId)
        val flowEvent = event.build()
        val p = new Put(Bytes.toBytes(rowKey))
        val sourceCol = getSourceCol(source, event.getEventId)
        p.addColumn(getSourceColumnFamily(source), sourceCol, flowEvent.toByteArray)
        puts.add(p)
      }
      traceContextTable.put(puts)
      events
    } finally {
      close(traceContextTable)
    }
  }

  /**
   * Extract information from instance, and must perform three steps in transaction:
   * 1. save all $variable updates to context
   * 2. if next event exists, put it to mailbox, with source set to null
   * 3. remove event from mailbox
   *
   * Besides, store can save instance to history for analyzing purpose
   */
  override def commitEvent(source: String, instance: FlowInstance.Builder): FlowRuntimeMessage.FlowInstanceOrBuilder = {
    val startTime = System.currentTimeMillis()
    val traceContextTable = conn.getTable(traceContextTableName)
    try {
      val event = instance.getEvent
      val traceId = event.getTraceId
      val rowKey = getRowKey(traceId)
      val rowKeyBytes = Bytes.toBytes(rowKey)
      val mutations = new RowMutations(rowKeyBytes)
      val sourceCF = getSourceColumnFamily(source)
      val sourceCol = getSourceCol(source, event.getEventId)

      //1. store to trace context
      val cxtPut = contextStore(instance)
      if (cxtPut.isDefined) {
        mutations.add(cxtPut.get)
      }

      //2. remove mailbox
      val del = new Delete(rowKeyBytes)
      del.addColumns(sourceCF, sourceCol)
      mutations.add(del)

      //3. atomic update row
      if (traceContextTable.checkAndMutate(rowKeyBytes, sourceCF, sourceCol, CompareOp.EQUAL,
        event.toByteArray, mutations)) {
        //成功
        //4. multi scheduler
        multiScheduler(source, instance)
        instance.build()
      } else {
        //失败
        val storeEvent = getMailboxEvent(traceContextTable, rowKeyBytes, sourceCF, sourceCol)
        val storeEventJson = JsonFormat.printer().omittingInsignificantWhitespace()
          .print(storeEvent.getOrElse(Empty.getDefaultInstance))
        val eventJson = JsonFormat.printer().omittingInsignificantWhitespace().print(event)
        error(("msg", "commit event cas error"), ("event store", storeEventJson), ("event exec", eventJson))
        null
      }
    } finally {
      close(traceContextTable)
      info(("msg", "commit event process time"), ("proc_time", s"${System.currentTimeMillis() - startTime}ms"))
    }
  }

  private def getMailboxEvent(traceContextTable: Table, rowKeyBytes: Array[Byte], cf: Array[Byte],
                              sourceCol: Array[Byte]): Option[FlowEvent] = {
    val get = new Get(rowKeyBytes)
    get.addColumn(cf, sourceCol)
    val result = traceContextTable.get(get)
    if (!result.isEmpty) {
      val byteValue = result.getValue(cf, sourceCol)
      val tryEvent = Try(FlowEvent.parseFrom(byteValue))
      if (tryEvent.isSuccess) Some(tryEvent.get) else None
    } else {
      None
    }
  }

  private def multiScheduler(source: String, instance: FlowInstance.Builder): Unit = {
    if (instance.getScheduleCount > 0) {
      val schedulerSourceTable = conn.getTable(schedulerTableName)
      try {
        val puts = ListBuffer[Put]()
        instance.getScheduleList().foreach(event => {
          val traceId = event.getTraceId
          val slot = getSlot(traceId, slotCount)
          val timestamp = if (event.getScheduledTimestamp == null || event.getScheduledTimestamp == 0) {
            System.currentTimeMillis()
          } else {
            event.getScheduledTimestamp
          }
          val rowkey = getMultiSchedulerRowKey(slot, timestamp, event.getEventId)
          val col = getMultiSchedulerCol(source)
          val put = new Put(Bytes.toBytes(rowkey))
          val newEvent = event.toBuilder.setScheduledTimestamp(timestamp).build()
          put.addColumn(schedulerSourceCF.getName, Bytes.toBytes(col), newEvent.toByteArray)
          puts.append(put)
        })
        schedulerSourceTable.put(puts)
      } finally {
        close(schedulerSourceTable)
      }
    }
  }

  private def contextStore(instance: FlowInstance.Builder): Option[Put] = {
    val event = instance.getEvent
    val traceId = event.getTraceId
    val contextMap = getCurrentContext(traceId)
    instance.getUpdateList.foreach(v => {
      val name = v.getReference.getName
      contextMap.put(name, v)
    })
    instance.getBackwardList.foreach(v => {
      val reference = ValueReference.newBuilder()
        .setEventId(event.getEventId)
        .setName(v.getLogId)
      val value = TraceVariable.newBuilder().setValue(
        FlowValue.newBuilder().setBinary(v.toByteString)
      ).setReference(reference).build()
      contextMap.put(v.getLogId, value)
    })
    instance.getTokenList.foreach(v => {
      val reference = ValueReference.newBuilder()
        .setEventId(event.getEventId)
        .setName(v.getToken)
      val value = TraceVariable.newBuilder().setValue(
        FlowValue.newBuilder().setBinary(v.toByteString)
      ).setReference(reference).build()
      contextMap.put(v.getToken, value)
    })

    val deletes = instance.getDeleteList.toSeq
    contextMap --= deletes

    if (contextMap.size > 0) {
      val rowKey = getRowKey(traceId)
      val rowKeyBytes = Bytes.toBytes(rowKey)
      val put = new Put(rowKeyBytes)
      val contextBuilder = TraceContext.newBuilder()
      for (value <- contextMap.values) {
        contextBuilder.addContexts(value)
      }
      put.addColumn(contextCF.getName, contextCol, contextBuilder.build().toByteArray)
      Some(put)
    } else {
      None
    }
  }

  /**
   * Get all pending events from source
   */
  override def getEventsBySource(source: String, beginSlot: Int, endSlot: Int):
  Iterable[FlowRuntimeMessage.FlowEventOrBuilder] = {
    val traceContextTable = conn.getTable(traceContextTableName)
    val results = ListBuffer[FlowRuntimeMessage.FlowEvent]()
    val sourceCF = getSourceColumnFamily(source)
    //val sourceCol = Bytes.toBytes(getSourceCol(source))
    val startRow = beginSlot.formatted("%05d")
    val endRow = endSlot.formatted("%05d")
    //scan 'HOROSCOPE:TRACE_CONTEXT', {STARTROW=>'00000', STOPROW=>'16000'}
    val scan = new Scan().addFamily(sourceCF).setFilter(new ColumnPrefixFilter(Bytes.toBytes(source.toUpperCase())))
      .withStartRow(Bytes.toBytes(startRow)).withStopRow(Bytes.toBytes(endRow)).setBatch(100)
    val scanner = traceContextTable.getScanner(scan)
    try {
      var result = scanner.next()
      while (result != null) {
        val rowkey = Bytes.toString(result.getRow)
        //filter slot flag
        if (rowkey.contains(ROWKEY_MARK)) {
          val cellScanner = result.cellScanner()
          while (cellScanner.advance()) {
            val cell = cellScanner.current()
            val value = CellUtil.cloneValue(cell)
            val flowEvent = FlowRuntimeMessage.FlowEvent.parseFrom(value)
            results.append(flowEvent)
          }
        }
        result = scanner.next()
      }
      results
    } finally {
      close(scanner)
      close(traceContextTable)
    }
  }

  /**
   * Get most recent snapshot of trace context
   */
  override def getContext(trace: String, keys: Array[String]): Future[Map[String, TraceVariableOrBuilder]] = {
    val p = Promise[Map[String, TraceVariableOrBuilder]]()
    Future {
      try {
        val values = mutable.Map[String, TraceVariableOrBuilder]()
        val contextMap = getCurrentContext(trace)
        //get key
        for (key <- keys) {
          val value = contextMap.get(key)
          if (value.isDefined) {
            values.put(key, value.get)
          }
        }
        p.success(values.toMap)
      } catch {
        case ex: Exception => p.failure(ex)
      }
    }
    p.future
  }

  private def getCurrentContext(trace: String): mutable.Map[String, TraceVariable] = {
    val traceContextTable = conn.getTable(traceContextTableName)
    try {
      val rowKey = getRowKey(trace)
      val contextMap = mutable.Map[String, TraceVariable]()
      val get = new Get(Bytes.toBytes(rowKey))
      get.addColumn(contextCF.getName, contextCol)
      val result = traceContextTable.get(get)
      if (!result.isEmpty) {
        val byteValue = result.getValue(contextCF.getName, contextCol)
        val context = TraceContext.parseFrom(byteValue)
        for (v <- context.getContextsList) {
          contextMap.put(v.getReference.getName, v)
        }
      }
      contextMap
    } finally {
      close(traceContextTable)
    }
  }

  /**
   * crc32(traceId) % 16384
   * 0-16383 slot
   *
   * @param traceId
   * @return
   */
  private def getRowKey(traceId: String): String = {
    val slot = Utils.getSlot(traceId, slotCount)
    val prefix = slot.formatted("%05d")
    val encodeTraceId = Utils.getTraceId(traceId)
    s"${prefix}${ROWKEY_MARK}${encodeTraceId}"
  }

  private def getSourceColumnFamily(source: String): Array[Byte] = {
    if (source.equals(schStoreCol(config))) {
      schedulerCF.getName
    } else {
      mailboxCF.getName
    }
  }

  /**
   * 将source name转换为hbase的colunm name
   *
   * @param source
   * @return
   */
  private def getSourceCol(source: String, eventId: String): Array[Byte] = {
    val name = s"$source${ROWKEY_MARK}$eventId".toUpperCase
    Bytes.toBytes(name)
  }

  private def acquireLock(traceId: String, lockInfo: String): String = {
    val tokenTable = conn.getTable(tokenTableName)
    try {
      doAcquireLock(tokenTable, traceId, lockInfo)
    } finally {
      close(tokenTable)
    }
  }

  private def doAcquireLock(tokenTable: Table, traceId: String, lockInfo: String): String = {
    val lockInfoBytes = md5LockInfo(lockInfo)
    val put = new Put(lockInfoBytes)
    put.addColumn(ownerCF.getName, ownerCol, Bytes.toBytes(traceId))
    if (tokenTable.checkAndPut(lockInfoBytes, ownerCF.getName,
      ownerCol, null, put)) {
      debug(("msg", "cas acquire lock success"))
      traceId
    } else {
      val owner = doGetLockOwner(tokenTable, lockInfo)
      if (owner.isEmpty) {
        warn(("msg", "cas acquire lock fail"), ("lockInfo", lockInfo))
        doAcquireLock(tokenTable, traceId, lockInfo)
      } else {
        owner.get
      }
    }
  }

  private def getLockOwner(lockInfo: String): Option[String] = {
    val tokenTable = conn.getTable(tokenTableName)
    try {
      doGetLockOwner(tokenTable, lockInfo)
    } finally {
      close(tokenTable)
    }
  }

  private def doGetLockOwner(tokenTable: Table, lockInfo: String): Option[String] = {
    val get = new Get(md5LockInfo(lockInfo))
    get.addColumn(ownerCF.getName, ownerCol)
    val result = tokenTable.get(get)
    debug(("msg", "get lock owner"), ("lockInfo", lockInfo), ("result", result))
    if (!result.isEmpty) {
      val owner = Bytes.toString(result.getValue(ownerCF.getName, ownerCol))
      Some(owner)
    } else {
      None
    }
  }

  private def returnLock(lockInfo: String): Unit = {
    val tokenTable = conn.getTable(tokenTableName)
    try {
      val del = new Delete(md5LockInfo(lockInfo))
      tokenTable.delete(del)
    } finally {
      close(tokenTable)
    }
  }

  def contextTable: Table = conn.getTable(traceContextTableName)

  def tokenTable: Table = conn.getTable(tokenTableName)

  private def md5LockInfo(lockInfo: String): Array[Byte] = {
    Bytes.toBytes(Hashing.md5().hashBytes(Bytes.toBytes(lockInfo)).toString)
  }

  /**
   * Get scheduler event
   */
  override def pollSchedulerEvents(source: String, slot: Int, timestamp: Long, limit: Int): Iterable[FlowEvent] = {
    val schedulerSourceTable = conn.getTable(schedulerTableName)
    val results = ListBuffer[FlowRuntimeMessage.FlowEvent]()
    val sourceCF = schedulerSourceCF.getName
    val sourceColumn = Bytes.toBytes(getMultiSchedulerCol(source))
    val startRow = Bytes.toBytes(getMultiSchedulerRowKey(slot, 0, ""))
    val stopRow = Bytes.toBytes(getMultiSchedulerRowKey(slot, timestamp, ""))
    val scan = new Scan().addColumn(sourceCF, sourceColumn)
      .withStartRow(startRow).withStopRow(stopRow).setBatch(limit).setLimit(limit)
    val scanner = schedulerSourceTable.getScanner(scan)
    try {
      val result = scanner.next(limit)
      if(result != null) {
        result.foreach(r => {
          val cellScanner = r.cellScanner()
          while (cellScanner.advance()) {
            val cell = cellScanner.current()
            val value = CellUtil.cloneValue(cell)
            val flowEvent = FlowRuntimeMessage.FlowEvent.parseFrom(value)
            results.append(flowEvent)
          }
        })
      }
      results
    } finally {
      close(scanner)
      close(schedulerSourceTable)
    }
  }

  override def commitSchedulerEvents(source: String, slot: Int, events: List[FlowEvent]): Long = {
    val schedulerSourceTable = conn.getTable(schedulerTableName)
    try {
      val sourceCF = schedulerSourceCF.getName
      val deletes = ListBuffer[Delete]()
      events.foreach(e => {
        //delete
        val rowkey = Bytes.toBytes(getMultiSchedulerRowKey(slot, e.getScheduledTimestamp, e.getEventId))
        val sourceColumn = Bytes.toBytes(getMultiSchedulerCol(source))
        val del = new Delete(rowkey)
        del.addColumns(sourceCF, sourceColumn)
        deletes.append(del)
      })
      schedulerSourceTable.delete(deletes)
      events.length
    } finally {
      close(schedulerSourceTable)
    }
  }

  private def getMultiSchedulerRowKey(slot: Int, timestamp: Long, eventId: String): String = {
    val prefix = slot.formatted("%05d")
    s"$prefix$ROWKEY_MARK$timestamp$ROWKEY_MARK$eventId"
  }

  private def getMultiSchedulerCol(source: String): String = {
    source.toUpperCase()
  }
}

object HBaseTraceStore {
  val mailboxCF = new HColumnDescriptor(Bytes.toBytes("MB"))
  val contextCF = new HColumnDescriptor(Bytes.toBytes("CX"))
  val schedulerCF = new HColumnDescriptor(Bytes.toBytes("SC"))
  val schedulerSourceCF = new HColumnDescriptor(Bytes.toBytes("SS"))
  val contextCol = Bytes.toBytes("CX")

  val ownerCF = new HColumnDescriptor(Bytes.toBytes("OW"))
  val ownerCol = Bytes.toBytes("OW")
}
