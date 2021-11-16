/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.storage

import java.nio.charset.Charset
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.get
import akka.http.scaladsl.server.Route
import com.alibaba.ttl.threadpool.TtlExecutors
import com.didichuxing.horoscope.core.FlowRuntimeMessage._
import com.didichuxing.horoscope.core.FlowRuntimeMessage
import com.didichuxing.horoscope.runtime.{Binary, Implicits, Value}
import com.didichuxing.horoscope.service.source.NamedThreadFactory
import com.didichuxing.horoscope.util.Constants.CONTEXT_VAL_FLAG
import com.didichuxing.horoscope.util.Utils.{close, getSlot}
import com.didichuxing.horoscope.util.{Logging, SystemLog, Utils}
import com.typesafe.config.Config
import redis.clients.jedis._

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try
import com.google.protobuf.util.JsonFormat

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
  var hashtag = false

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
    hashtag = Try(config.getBoolean("horoscope.redis.hashtag")).getOrElse(false)
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
      }).foreach {
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
      //2. context
      context(instance, pipeline)
      //3. del
      val mailboxKey = getMailboxKey(source, getSlot(traceId, slotCount))
      pipeline.hdel(toBytes(mailboxKey), toBytes(event.getEventId))
      //4. multi scheduler
      scheduler(source, instance, pipeline)
      //同步执行
      pipeline.sync()
    } catch {
      case e: Throwable =>
        error(s"got an exception when commit event to redis, ${e.getMessage}")
    } finally {
      close(jedis)
    }
    instance.build()
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
    // log backward context
    instance.getBackwardList.foreach(b => {
      val logId = toBytes(b.getLogId)
      val value = TraceVariable.newBuilder().setValue(
        Binary(b.toByteArray).as[FlowValue]
      ).build().toByteArray
      contexts.put(logId, value)
    })
    // callback token context
    instance.getTokenList.foreach(t => {
      val token = toBytes(t.getToken)
      val value = TraceVariable.newBuilder().setValue(
        Binary(t.toByteArray).as[FlowValue]
      ).build().toByteArray
      contexts.put(token, value)
    })

    val contextKey = toBytes(getContextKey(traceId))
    pipeline.hmset(contextKey, contexts)
    // delete trace context keys
    val deletes = instance.getDeleteList.toSeq.map(toBytes)
    if (deletes.nonEmpty) pipeline.hdel(contextKey, deletes: _*)
    pipeline.expire(contextKey, contextTTL)
  }

  private def scheduler(source: String, instance: FlowInstance.Builder, pipeline: Pipeline): Unit = {
    if (instance.getScheduleCount > 0) {
      val events = instance.getScheduleList
      events.groupBy(event => {
        val traceId = event.getTraceId
        val slotId = getSlot(traceId, slotCount)
        info(("msg", "schedule slot"), ("source", source), ("slot_id", slotId))
        getSchedulerKey(source, slotId)
      }).foreach {
        case (key, events) => {
          events.foreach(e => {
            val timestamp = if (!e.hasScheduledTimestamp || e.getScheduledTimestamp == 0) {
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
        resp._2.get().foreach(e => {
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
    if (events.size > 0) {
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
    if (hashtag) {
      s"{$prefix}:$traceId"
    } else {
      s"$prefix:$traceId"
    }
  }

  private def getSchedulerKey(source: String, slot: Int): String = {
    val prefix = slot.formatted("%05d")
    if (hashtag) {
      s"{$prefix}:$source:sc"
    } else {
      s"$prefix:$source:sc"
    }
  }

  private def getMailboxKey(source: String, slot: Int): String = {
    val prefix = slot.formatted("%05d")
    if (hashtag) {
      s"{$prefix}:$source:mb"
    } else {
      s"$prefix:$source:mb"
    }
  }

  //scalastyle:off
  override def api(): Route = {
    import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
    import akka.http.scaladsl.server.Directives._
    import spray.json._
    import DefaultJsonProtocol._
    import com.didichuxing.horoscope.runtime.convert.FlowValueConverter._
    pathPrefix("redis") {
      concat(
        get {
          path("count" / Segment / Remaining) { (table, source) =>
            SystemLog.create()
            info(("msg", "total size"), ("table", table), ("source", source))
            var jedis: Jedis = null
            complete(
              try {
                var total = 0L
                jedis = pool.getResource
                for (slot <- 0 until slotCount) {
                  table match {
                    case "mb" =>
                      val key = getMailboxKey(source, slot)
                      total += jedis.hlen(key)
                    case "sc" =>
                      val key = getSchedulerKey(source, slot)
                      total += jedis.zcount(key, 0, Long.MaxValue)
                    case _ =>
                  }
                }
                s"$total"
              } finally {
                close(jedis)
              }
            )
          }
        },
        get {
          path("slots" / Segment / Remaining) { (table, source) =>
            var jedis: Jedis = null
            complete(
              try {
                var details: Map[String, Long] = Map()
                jedis = pool.getResource
                for (slot <- 0 until slotCount) {
                  table match {
                    case "mb" =>
                      val key = getMailboxKey(source, slot)
                      val slotCount = jedis.hlen(key)
                      details += (slot.toString -> slotCount)
                    case "sc" =>
                      val key = getSchedulerKey(source, slot)
                      val slotCount = jedis.zcount(key, 0, Long.MaxValue)
                      details += (slot.toString -> slotCount)
                    case _ =>
                  }
                }
                details.toJson
              } finally {
                close(jedis)
              }
            )
          }
        },
        delete {
          path("flush" / Segment / Remaining) { (table, source) =>
            SystemLog.create()
            info(("msg", "flush redis storage"), ("table", table), ("source", source))
            var jedis: Jedis = null
            complete(
              try {
                jedis = pool.getResource
                val keys = ListBuffer[String]()
                for (slot <- 0 until slotCount) {
                  val key = table match {
                    case "sc" =>
                      getSchedulerKey(source, slot)
                    case "mb" =>
                      getMailboxKey(source, slot)
                    case _ =>
                      ""
                  }
                  if (!key.equals("")) {
                    debug(("delete key", key))
                    keys.append(key)
                  }
                }
                jedis.del(keys: _*)
                StatusCodes.OK
              } finally {
                close(jedis)
              }
            )
          }
        },
        delete {
          path("trace" / Remaining) { traceId =>
            SystemLog.create()
            info(("msg", "remove redis trace"), ("traceId", traceId))
            var jedis: Jedis = null
            complete(
              try {
                jedis = pool.getResource
                val key = getContextKey(traceId)
                debug(("delete key", key))
                jedis.del(key)
                StatusCodes.OK
              } finally {
                close(jedis)
              }
            )
          }
        },
        get {
          path("trace" / Remaining) { traceId =>
            SystemLog.create()
            info(("msg", "get redis trace"), ("traceId", traceId))
            var jedis: Jedis = null
            complete(
              try {
                jedis = pool.getResource
                val context = getCurrentContext(traceId).map {
                  case (key, value) =>
                    if (key.startsWith("#")) {
                      (key, Implicits.gson.toJson(
                        Value(TokenContext.parseFrom(value.getValue.getBinaryValue))).parseJson)
                    } else if (key.startsWith("&")) {
                      (key, Implicits.gson.toJson(
                        Value(BackwardContext.parseFrom(value.getValue.getBinaryValue))).parseJson)
                    } else {
                      (key, Implicits.gson.toJson(Value(value.getValue)).parseJson)
                    }
                }
                context.toMap.toJson
              } finally {
                close(jedis)
              }
            )
          }
        },
        get {
          path("sample" / Segment / Segment / Remaining) { (table, source, slot) =>
            var jedis: Jedis = null
            complete(
              try {
                jedis = pool.getResource
                val events = table match {
                  case "mb" => getEventsBySource(source, slot.toInt, slot.toInt + 1).take(10)
                  case "sc" => pollSchedulerEvents(source, slot.toInt, System.currentTimeMillis(), 10)
                  case _ => Nil
                }
                events.map(e => Implicits.gson.toJson(Value(e)).parseJson).toList
              } finally {
                close(jedis)
              }
            )
          }
        },
        get {
          complete(s"redis storage")
        }
      )
    }
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
}
