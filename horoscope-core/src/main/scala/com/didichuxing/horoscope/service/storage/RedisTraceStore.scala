/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.storage

import java.nio.charset.Charset
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import com.alibaba.ttl.threadpool.TtlExecutors
import com.didichuxing.horoscope.core.FlowRuntimeMessage._
import com.didichuxing.horoscope.core.{FlowRuntimeMessage}
import com.didichuxing.horoscope.service.source.NamedThreadFactory
import com.didichuxing.horoscope.util.Constants.CONTEXT_VAL_FLAG
import com.didichuxing.horoscope.util.Utils.{close, getSlot}
import com.didichuxing.horoscope.util.{Logging, Utils}
import com.typesafe.config.Config
import redis.clients.jedis._

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

/**
 * mailbox:  {slot}:source:mb, map[eventId, event]
 * context:  {slot}:traceId, map[name, context]  ttl 7day
 * scheduler: {slot}:source:sc, timestamp, event
 *
 * @param executionContext
 */
class RedisTraceStore(executionContext: ExecutionContext = null) extends AbstractTraceStore with Logging {

  import RedisTraceStore._

  implicit val ec = if (executionContext == null) {
    val pool = new ThreadPoolExecutor(2, 20, 1000L, TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue[Runnable](1000), new NamedThreadFactory("hbaseTraceStore"))
    ExecutionContext.fromExecutorService(TtlExecutors.getTtlExecutorService(pool))
  } else {
    executionContext
  }

  var pool: JedisPool = _
  var contextTTL: Int = _
  var slotCount = 0

  override def start(conf: Config): Unit = {
    super.start(conf)
    val host = Try(config.getString("horoscope.redis.host")).getOrElse("127.0.0.1")
    val port = Try(config.getInt("horoscope.redis.port")).getOrElse(6379)
    val maxTotal = Try(config.getInt("horoscope.redis.max-total")).getOrElse(16)
    val maxIdle = Try(config.getInt("horoscope.redis.max-idle")).getOrElse(16)
    val minIdle = Try(config.getInt("horoscope.redis.min-idle")).getOrElse(0)
    val poolConfig = new JedisPoolConfig()
    poolConfig.setMaxTotal(maxTotal)
    poolConfig.setMaxIdle(maxIdle)
    poolConfig.setMinIdle(minIdle)
    poolConfig.setBlockWhenExhausted(true)
    poolConfig.setMaxWaitMillis(5000)
    pool = new JedisPool(poolConfig, host, port)
    contextTTL = Try(config.getInt("horoscope.redis.context-ttl")).getOrElse(604800)
    slotCount = Utils.getClusterSlotCount(config)
  }

  override def stop(): Unit = {
    close(pool)
  }

  /**
   * Add a new event to store, try to acquire and return token when needed
   */
  override def addEvent(source: String, event: FlowEvent.Builder): FlowEventOrBuilder = {
    var jedis: Jedis = null
    try {
      jedis = pool.getResource
      val flowEvent = event.build()
      val traceId = event.getTraceId
      val key = getMailboxKey(source, getSlot(traceId, slotCount))
      jedis.hset(toBytes(key), toBytes(event.getEventId), flowEvent.toByteArray)
      flowEvent
    } finally {
      close(jedis)
    }
  }

  /**
   * Batch add new events to store
   */
  override def addEvents(source: String, events: List[FlowEvent.Builder]): List[FlowEventOrBuilder] = {
    var jedis: Jedis = null
    try {
      jedis = pool.getResource
      events.groupBy(event => {
        val traceId = event.getTraceId
        getMailboxKey(source, getSlot(traceId, slotCount))
      }).map {
        case (key, events) => {
          val flowEvents = mutable.Map[Array[Byte], Array[Byte]]()
          events.foreach(event => {
            flowEvents.put(toBytes(event.getEventId), event.build().toByteArray)
          })
          jedis.hmset(toBytes(key), flowEvents)
        }
      }
      events
    } finally {
      close(jedis)
    }
  }

  /**
   * Extract information from instance, and must perform three steps in transaction:
   * 1. save all $variable updates to context
   * 2. if next event exists, put it to mailbox, with source set to null
   * 3. remove event from mailbox
   *
   * Besides, store can save instance to history for analyzing purpose
   * https://www.cnblogs.com/danic/p/10736046.html
   * https://www.jianshu.com/p/366d1b4f0d13
   * https://www.runoob.com/lua/lua-tables.html
   */
  override def commitEvent(source: String, instance: FlowInstance.Builder): FlowInstanceOrBuilder = {
    var jedis: Jedis = null
    try {
      jedis = pool.getResource
      val pipeline = jedis.pipelined()
      val event = instance.getEvent
      val traceId = event.getTraceId
      //1. goto
      goto(source, instance, pipeline)
      //2. context
      context(instance, pipeline)
      //3. del
      val mailboxKey = getMailboxKey(source, getSlot(traceId, slotCount))
      pipeline.hdel(toBytes(mailboxKey), toBytes(event.getEventId))
      //4. multi scheduler
      scheduler(source, instance, pipeline)
      //同步执行
      pipeline.sync()
    } finally {
      close(jedis)
    }
    instance
  }

  private def goto(source: String, instance: FlowInstance.Builder, pipeline: Pipeline): Unit = {
    if (instance.hasGoto) {
      val goto = instance.getGoto
      if (goto.hasScheduledTimestamp && goto.getScheduledTimestamp > 0) {
        //延迟执行
        val schSource = Utils.schStoreCol(config)
        val gotoKey = getMailboxKey(schSource, getSlot(goto.getTraceId, slotCount))
        pipeline.hset(toBytes(gotoKey), toBytes(goto.getEventId), goto.toByteArray)
      } else {
        //立即执行
        val gotoKey = getMailboxKey(source, getSlot(goto.getTraceId, slotCount))
        pipeline.hset(toBytes(gotoKey), toBytes(goto.getEventId), goto.toByteArray)
      }
    }
  }

  private def context(instance: FlowInstance.Builder, pipeline: Pipeline): Unit = {
    val event = instance.getEvent
    val traceId = event.getTraceId
    val contexts = new java.util.HashMap[Array[Byte], Array[Byte]]()
    //context v2
    instance.getUpdateList.foreach(v => {
      val name = toBytes(v.getReference.getName)
      val value = v.toByteArray
      contexts.put(name, value)
    })
    //context v1
    instance.getAssignList.foreach(assign => {
      val name = assign.getName
      if (name.startsWith(CONTEXT_VAL_FLAG)) {
        val flowValue = assign.getValue
        val valueRef = ValueReference.newBuilder().setEventId(event.getEventId).setName(name).build()
        val traceVal = TraceVariable.newBuilder().setReference(valueRef).setValue(flowValue).build()
        contexts.put(toBytes(name), traceVal.toByteArray)
      }
    })
    val contextKey = toBytes(getContextKey(traceId))
    pipeline.hmset(contextKey, contexts)
    pipeline.expire(contextKey, contextTTL)
  }

  private def scheduler(source: String, instance: FlowInstance.Builder, pipeline: Pipeline): Unit = {
    if (instance.getScheduleCount > 0) {
      val events = instance.getScheduleList
      events.groupBy(event => {
        val traceId = event.getTraceId
        getSchedulerKey(source, getSlot(traceId, slotCount))
      }).map {
        case (key, events) => {
          events.foreach(e => {
            val timestamp = if (e.getScheduledTimestamp == null || e.getScheduledTimestamp == 0) {
              System.currentTimeMillis()
            } else {
              e.getScheduledTimestamp
            }
            val newEvent = e.toBuilder.setScheduledTimestamp(timestamp).build()
            pipeline.zadd(toBytes(key), timestamp, newEvent.toByteArray)
          })
        }
      }
    }
  }

  /**
   * Get all pending events from source
   */
  override def getEventsBySource(source: String, beginSlot: Int, endSlot: Int): Iterable[FlowEventOrBuilder] = {
    var jedis: Jedis = null
    try {
      jedis = pool.getResource
      val responses = mutable.Map[Int, Response[java.util.Map[Array[Byte], Array[Byte]]]]()
      val pipeline = jedis.pipelined()
      for (slot <- beginSlot until endSlot) {
        val key = getMailboxKey(source, slot)
        responses.put(slot, pipeline.hgetAll(toBytes(key)))
        //是否有必要设置ttl
        //pipeline.expire(toBytes(key), TTL)
      }
      pipeline.sync()
      val events = ListBuffer[FlowEvent]()
      responses.foreach(resp => {
        resp._2.get().map(e => {
          val flowEvent = FlowRuntimeMessage.FlowEvent.parseFrom(e._2)
          events.append(flowEvent)
        })
      })
      events
    } finally {
      close(jedis)
    }
  }

  /**
   * Get most recent snapshot of trace context
   */
  override def getContext(trace: String, keys: Array[String]): Future[Map[String, TraceVariableOrBuilder]] = {
    val p = Promise[Map[String, TraceVariableOrBuilder]]()
    Future {
      val contexts = getCurrentContext(trace)
      p.success(contexts.toMap)
    }
    p.future
  }

  private def getCurrentContext(trace: String): mutable.Map[String, TraceVariable] = {
    val values = mutable.Map[String, TraceVariable]()
    var jedis: Jedis = null
    try {
      jedis = pool.getResource
      val values = mutable.Map[String, TraceVariable]()
      jedis.hgetAll(toBytes(getContextKey(trace))).foreach {
        case (byteName, byteValue) => {
          val name = new String(byteName)
          val value = TraceVariable.parseFrom(byteValue)
          values.put(name, value.toBuilder.build())
        }
      }
      values
    } catch {
      case ex: Exception =>
        error(("msg", ex.getMessage))
        values
    } finally {
      close(jedis)
    }
  }

  /**
   * Poll schedule event
   */
  override def pollSchedulerEvents(source: String, slot: Int, timestamp: Long, limit: Int): Iterable[FlowEvent] = {
    var jedis: Jedis = null
    try {
      jedis = pool.getResource
      val events = ListBuffer[FlowEvent]()
      val key = getSchedulerKey(source, slot)
      jedis.zrangeByScore(toBytes(key), 0, timestamp, 0, limit).foreach(e => {
        val flowEvent = FlowRuntimeMessage.FlowEvent.parseFrom(e)
        events.append(flowEvent)
      })
      events
    } finally {
      close(jedis)
    }
  }

  override def commitSchedulerEvents(source: String, slot: Int, events: List[FlowEvent]): Long = {
    if(events.size > 0) {
      var jedis: Jedis = null
      try {
        jedis = pool.getResource
        val key = getSchedulerKey(source, slot)
        jedis.zrem(toBytes(key), events.map(e => e.toByteArray): _*)
      } finally {
        close(jedis)
      }
    } else {
      0
    }
  }

  private def getContextKey(traceId: String): String = {
    val slot = getSlot(traceId, slotCount)
    val prefix = slot.formatted("%05d")
    s"{$prefix}:$traceId"
  }
}

object RedisTraceStore {

  val script =
    """
      | local count = ARGV[1]
      | for i=1, count do
      |   redis.call("hmset", KEYS[i], ARGV[i+1])//schedule add e1
      | end
      | redis.call("hmset", KEYS[count+1], ARGV[count+2])//context update
      | redis.call("expire", KEYS[count+1], ARGV[count+3])//context expire ttl
      | redis.call("hdel", KEYS[count+2], ARGV[count+4])//del mailbox
      | return 0
      |""".stripMargin

  private def toBytes(s: String): Array[Byte] = s.getBytes(Charset.forName("UTF-8"))

  private def getSchedulerKey(source: String, slot: Int): String = {
    val prefix = slot.formatted("%05d")
    s"{$prefix}:$source:sc"
  }

  private def getMailboxKey(source: String, slot: Int): String = {
    val prefix = slot.formatted("%05d")
    s"{$prefix}:$source:mb"
  }
}
