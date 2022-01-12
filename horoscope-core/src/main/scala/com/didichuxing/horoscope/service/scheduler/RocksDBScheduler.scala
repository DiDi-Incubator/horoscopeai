/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.scheduler

import java.io.File
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}

import akka.actor.{Actor, ActorRef, Props}
import com.didichuxing.horoscope.core.FlowRuntimeMessage._
import com.didichuxing.horoscope.core.{FlowRuntimeMessage, SourceFactory}
import com.didichuxing.horoscope.runtime.FlowExecutor
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.service.resource.{ResourceManager, SlotRange}
import com.didichuxing.horoscope.service.source.{SchedulerEventBus, SchedulerSource}
import com.didichuxing.horoscope.util.Constants._
import com.didichuxing.horoscope.util.Utils._
import com.didichuxing.horoscope.util.{Logging, SystemLog, Utils}
import com.google.common.io.Files
import com.typesafe.config.Config
import org.apache.hadoop.hbase.util.Bytes
import org.rocksdb.{BlockBasedTableConfig, Options, ReadOptions, RocksDB}

import scala.concurrent.Future

/**
 * http://wiki.intra.xiaojukeji.com/pages/viewpage.action?pageId=322779383
 */
@deprecated(since = "0.6.3")
class RocksDBScheduler(implicit ctx: ApplicationContext) extends Scheduler with Logging {

  implicit val config = ctx.config
  implicit val traceStore = ctx.traceStore
  implicit val system = ctx.system
  implicit val ec = ctx.sourceExecutionContext.getExecutionContext()
  var rocksDB: RocksDB = _
  var rocksOption: Options = _
  var delaySource: SchedulerSource = _
  var schedulerActor: ActorRef = _
  val recoveryLock = new ReentrantLock(true) //保证不同时执行多个recovery
  val onTimeFlag = new AtomicBoolean(false) //是否正在执行onTime任务
  val resetRocksDBLock = new ReentrantReadWriteLock(true) //reset db的时候不能读写，读写的时候不能reset
  val batchSize = Utils.getSlotBatchSize(ctx.config)

  override def start(flowExecutor: FlowExecutor, sourceFactories: Map[String, SourceFactory]): Unit = {
    val schSourceFactory = sourceFactories.get(SCH_SOURCE_FACTORY)
    if (schSourceFactory.isDefined) {
      //start rocksdb
      RocksDB.loadLibrary()
      rocksOption = new Options().setCreateIfMissing(true)
      val tableFormatConfig = new BlockBasedTableConfig()
      //  val blockCacheSize = 100 * 1024 * 1024 = 100M
      val cacheSize = configIntOrDefault(config, "horoscope.scheduler.rocksdb.cache", 100)
      tableFormatConfig.setBlockCacheSize(1048576 * cacheSize) //block cache size，系统默认8M
      tableFormatConfig.setCacheNumShardBits(10) //分段数2的10次方，增加并发能力
      rocksOption.setTableFormatConfig(tableFormatConfig)
      startRocksDB()
      val delayConfig = config.getConfig("horoscope.scheduler.delay")
      delaySource = startSchedulerSource(flowExecutor, schSourceFactory.get, delayConfig, SCH_DELAY_SOURCE)
      //start receive event
      schedulerActor = system.actorOf(Props(new SchedulerActor), "SchedulerActor")
      debug("scheduler start success")
    } else {
      error("scheduler init error, scheduler source factory not found!")
    }
  }

  override def stop(): Unit = {
    if (delaySource != null) {
      delaySource.stop()
    }
    if (rocksDB != null) {
      rocksDB.close()
    }
    if (rocksOption != null) {
      rocksOption.close()
    }
    debug("scheduler stop")
  }

  /**
   * 外部时间触发
   *
   * @param timestamp
   */
  override def onTimeSpanSwitch(timestamp: Long): Unit = {
    SystemLog.create()
    debug(("msg", "onTimeSpanSwitch"), ("timestamp", timestamp))
    if (onTimeFlag.compareAndSet(false, true)) {
      val f = Future[Unit] {
        val startTime = System.currentTimeMillis()
        resetRocksDBLock.readLock().lock()
        val snapshot = rocksDB.getSnapshot
        val readOptions = new ReadOptions
        if (snapshot != null) {
          readOptions.setSnapshot(snapshot)
        }
        var count = 0
        val iter = rocksDB.newIterator(readOptions)
        try {
          iter.seekForPrev(Bytes.toBytes(s"$timestamp"))
          while (iter.isValid) {
            count += 1
            delaySource.push(List(iter.value()))
            rocksDB.delete(iter.key())
            iter.prev()
          }
        } catch {
          case ex: Exception =>
            ex.printStackTrace()
            error(("msg", "onTimeSpanSwitch error"), ("ex", ex.getMessage))
        } finally {
          if (snapshot != null) {
            rocksDB.releaseSnapshot(snapshot)
            snapshot.close()
          }
          readOptions.close()
          iter.close()
          resetRocksDBLock.readLock().unlock()
          val endTime = System.currentTimeMillis()
          info(("msg", "onTimeSpanSwitch success"), ("count", count), ("proc_time", s"${endTime - startTime}ms"))
        }
      }
      f.onComplete {
        case _ =>
          onTimeFlag.set(false)
          debug("onTimeSpanSwitch complete")
      }
    } else {
      error(("msg", "scheduler onTimeSpanSwitch is running"), ("timestamp", timestamp))
    }
  }

  override def goto(event: FlowRuntimeMessage.FlowEvent): Unit = {
    schedulerActor ! event
  }

  /**
   * 异步加载，由resourceManager在分区后调用
   */
  override def recovery(resourceManager: ResourceManager): Unit = {
    debug(("msg", "scheduler recovery"))
    val f = Future[Unit] {
      recoveryLock.lock()
      try {
        //通过当前集群参与者，计算归属的slot
        val slotRange = resourceManager.getSlotRange(resourceManager.local())
        if (slotRange.isEmpty) {
          error(("msg", "scheduler recovery not found slot range"))
        } else {
          val beginTime = System.currentTimeMillis()
          //可能会删除正在运行的actor的消息，需要加锁。
          resetRocksDB()
          //加载相关slot数据
          concurrentGetEventsBySource(slotRange.get)
          val endTime = System.currentTimeMillis()
          info(("msg", "scheduler recovery process time"), ("proc_time", s"${endTime - beginTime}ms"))
        }
      } finally {
        recoveryLock.unlock()
      }
    }
    f.onSuccess {
      case _ =>
        debug("scheduler recovery complete")
    }
  }

  def concurrentGetEventsBySource(range: SlotRange): Unit = {
    //[begin, end)左闭右开
    val total = range.end - range.begin
    val rest = total % batchSize
    val count = if (rest == 0) total / batchSize else (total / batchSize) + 1
    val cdl = new CountDownLatch(count)
    for (i <- 0 until count) {
      val begin = range.begin + (i * batchSize)
      val offset = if (i != (count - 1) | rest == 0) batchSize else rest
      ec.execute(new GetEventsBySourceThread(begin, begin + offset, cdl))
    }
    if (!cdl.await(30, TimeUnit.SECONDS)) {
      error(("msg", "getEventsBySource timeout"), ("begin", range.begin), ("end", range.end))
    } else {
      info(("msg", "scheduler recovery success"), ("begin", range.begin), ("end", range.end))
    }
  }

  private class GetEventsBySourceThread(begin: Int, end: Int, cdl: CountDownLatch) extends Runnable {
    override def run(): Unit = {
      try {
        val beginTime = System.currentTimeMillis()
        val flowEvents = traceStore.getEventsBySource(schStoreCol(config), begin, end)
        for (event <- flowEvents.map(f => f.asInstanceOf[FlowEvent])) {
          if (event.hasScheduledTimestamp && event.getScheduledTimestamp > 0) {
            val rowKey = getRowKey(event)
            rocksDB.put(Bytes.toBytes(rowKey), event.toByteArray)
          }
        }
        val endTime = System.currentTimeMillis()
        info(("msg", "scheduler recovery segment success"), ("begin", begin), ("end", end),
          ("size", flowEvents.size), ("proc_time", s"${endTime - beginTime}ms"))
      } catch {
        case ex: Exception =>
          error(("msg", "getEventsBySource error"), ("begin", begin), ("end", end), ("ex", ex.getMessage))
      } finally {
        cdl.countDown()
      }
    }
  }

  /**
   * 启动一个内部的scheduler数据源
   */
  private def startSchedulerSource(flowExecutor: FlowExecutor, sourceFactory: SourceFactory, params: Config,
                                   sourceName: String): SchedulerSource = {
    //config.getInt("backpress")
    val source = sourceFactory.newSource(params)
    source.start(new SchedulerEventBus(schStoreCol(config), params))
    info(("msg", s"$sourceName has start"))
    source.asInstanceOf[SchedulerSource]
  }

  private def startRocksDB(): Unit = {
    val dbPath = config.getString("horoscope.scheduler.rocksdb.path")
    Files.createParentDirs(new File(dbPath))
    //每次重启是先初始化db
    RocksDB.destroyDB(dbPath, rocksOption)
    rocksDB = RocksDB.open(rocksOption, dbPath)
    debug(("msg", "start rocks db"), ("path", dbPath))
  }

  private def resetRocksDB(): Unit = {
    resetRocksDBLock.writeLock().lock()
    try {
      //删除全部数据,deleteRange性能如何？
      //https://github.com/facebook/rocksdb/wiki/DeleteRange-Implementation
      //rocksDB.deleteRange(Bytes.toBytes("0000000000000"), Bytes.toBytes("9999999999999"))
      rocksDB.close()
      startRocksDB()
    } finally {
      resetRocksDBLock.writeLock().unlock()
    }
  }

  private def getRowKey(flowEvent: FlowEvent): String = {
    val traceId = flowEvent.getTraceId
    val gotoTime = flowEvent.getScheduledTimestamp
    val encodeTraceId = Utils.getTraceId(traceId)
    s"$gotoTime$ROWKEY_MARK$encodeTraceId"
  }

  class SchedulerActor extends Actor {
    override def receive: Receive = {
      case event: FlowEvent =>
        if (event.hasScheduledTimestamp && event.getScheduledTimestamp > 0) {
          //save to rocksdb,如果正在reset db，自旋，直到reset结束后才能写入
          resetRocksDBLock.readLock().lock()
          try {
            val rowKey = getRowKey(event)
            rocksDB.put(Bytes.toBytes(rowKey), event.toByteArray)
          } finally {
            resetRocksDBLock.readLock().unlock()
          }
        } else {
          //immediately process
        }
    }
  }

}
