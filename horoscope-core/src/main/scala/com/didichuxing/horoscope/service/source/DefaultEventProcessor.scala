/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import java.util.concurrent.{ConcurrentHashMap, Semaphore, TimeUnit}

import com.didichuxing.horoscope.core.FlowRuntimeMessage.{FlowEvent, FlowInstance}
import com.didichuxing.horoscope.core.TraceStore
import com.didichuxing.horoscope.runtime.convert.ValueTypeAdapter
import com.didichuxing.horoscope.runtime.{FlowExecutor, IgnoredException, Value}
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.service.source.EventProcessErrorCode._
import com.didichuxing.horoscope.util.Utils._
import com.didichuxing.horoscope.util.{Logging, PublicLog, Utils}
import com.google.common.util.concurrent.RateLimiter
import com.google.gson.{Gson, GsonBuilder}
import com.google.protobuf.util.JsonFormat
import com.typesafe.config.Config

import scala.collection.mutable.ListBuffer
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class DefaultEventProcessor(sourceName: String, params: Config)
                           (implicit ctx: ApplicationContext) extends EventProcessor with Logging {

  implicit val ec: ExecutionContextExecutorService = ctx.sourceExecutionContext.getExecutionContext()
  val storeService: TraceStore = ctx.traceStore
  val flowExecutor: FlowExecutor = ctx.flowExecutor
  //反压超时时间，默认10秒
  val backPressTimeout: Int = configIntOrDefault(params, "backpress.timeout", 10)
  // 限流值, 默认10QPS
  val backpressPermit: Int = configIntOrDefault(params, "backpress.permits", 10)
  // 取值true时, 反压拒绝生效, 消息直接丢弃
  val backpressReject: Boolean = Try(params.getBoolean("backpress.reject")).getOrElse(false)
  val routineRecovery: Boolean = Try(params.getBoolean("recovery.enabled")).getOrElse(false)
  val recoveryIntervalInSecond: Int = Try(params.getInt("recovery.interval")).getOrElse(300)
  var rateLimiter: RateLimiter = _
  //pb转json
  val jsonPrinter: JsonFormat.Printer = JsonFormat.printer.omittingInsignificantWhitespace()
  implicit val gson: Gson = new GsonBuilder()
    .registerTypeHierarchyAdapter(classOf[Value], new ValueTypeAdapter)
    .create()
  //retry eventId:retryCount
  val retryFlowEvents = new ConcurrentHashMap[String, Integer]()
  //最大重试次数, 默认重试1次
  val maxRetryCount: Int = configIntOrDefault(params, "exec.retry", 1)
  //反压超时决绝服务日志
  val rejectLog = new PublicLog("map_traffic_horoscope_reject")

  override def start(): Unit = {
    rateLimiter = RateLimiter.create(backpressPermit)
  }

  override def stop(): Unit = {}

  override def putEvent(events: List[FlowEvent]): List[FlowEvent] = {
    // 获取反压令牌
    val eventSize = events.size
    val beginTime = System.currentTimeMillis()
    val acquireResult = rateLimiter.tryAcquire(eventSize, backPressTimeout, TimeUnit.SECONDS)
    if (acquireResult) {
      val acquireTime = System.currentTimeMillis()
      info(("msg", "acquire permit time"), ("source", sourceName), ("proc_time", s"${acquireTime - beginTime} ms"))
      var successMailbox = false
      try {
        // 批量放入mailbox
        storeService.addEvents(sourceName, events.map(v => v.toBuilder))
        successMailbox = true
        val endTime = System.currentTimeMillis()
        info(("msg", "add mailbox time"), ("source", sourceName), ("proc_time", s"${endTime - acquireTime} ms"))
      } catch {
        case ex: Exception =>
          error(("msg", "add event to mailbox error"), ("ex", ex.getMessage))
      }
      if (successMailbox) {
        val results = ListBuffer[FlowEvent]()
        // 执行event
        for (flowEvent <- events) {
          exec(flowEvent)
          results.append(flowEvent)
        }
        // 返回成功的event
        results.toList
      } else {
        error(("msg", "add mailbox error"), ("source", sourceName), ("event_size", eventSize))
        new Array[FlowEvent](eventSize).toList
      }
    } else {
      error(("msg", "add mailbox error backpress timeout"), ("source", sourceName), ("event_size", eventSize))
      if (backpressReject) {
        for (event <- events) {
          rejectLog.public(("flow_event", Value(event).toJson))
        }
        events
      } else {
        new Array[FlowEvent](eventSize).toList
      }
    }
  }

  def exec(flowEvent: FlowEvent) {
    // flow execute callback
    flowExecutor.execute(flowEvent).onComplete {
      case Success(flowInstance) =>
        onSuccess(flowInstance)
      case Failure(exception) =>
        onFail(flowEvent, exception)
    }
    debug(("msg", "event execute"), ("event", Value(flowEvent).toJson))
  }

  def onSuccess(flowInstance: FlowInstance): Unit = {
    val flowEvent = flowInstance.getEvent
    try {
      if (commitCheck(flowEvent)) {
        val fib = storeService.commitEvent(sourceName, flowInstance.toBuilder)
        if (fib == null) {
          error(("msg", "commit check event error"), ("instance", Value(flowInstance).toJson))
        } else {
          info(("msg", "event execute success"), ("source", sourceName),
            ("duration", s"${fib.getEndTime - fib.getStartTime} ms"))
          // 打印ODS分析日志
          ctx.odsLogger.log(flowInstance)
        }
      } else {
        error(("msg", "commit check error"), ("source", sourceName), ("event", Value(flowEvent).toJson))
        //对于source消息, 采用每隔一段时间扫描mailbox，发现有长时间未被执行的消息重新执行
      }
    } catch {
      case ex: Exception =>
        //执行成功，但是commit失败，尝试重新执行
        ex.printStackTrace()
        error(("msg", "commit pendding event error"), ("exception", ex.getMessage))
        onFail(flowEvent, ex)
    }
  }

  def commitCheck(flowEvent: FlowEvent): Boolean = {
    true
  }

  def onFail(flowEvent: FlowEvent, exception: Throwable): Unit = {
    exception match {
      case ex: IgnoredException =>
        // just ignore and commit
        warn(("msg", "executor event with ignored exception"), ("ex", printStackTraceStr(ex)),
          ("event", Value(flowEvent).toJson))
        ignoreFailEvent(flowEvent)

      case err: ExecutionException if err.getCause.isInstanceOf[Error] =>
        // Error异常不重试，如：不属于该分区
        error(("msg", "executor event error"), ("ex", printStackTraceStr(err)),
          ("event", Value(flowEvent).toJson))

      case ex: Throwable =>
        error(("msg", "executor event error"), ("ex", printStackTraceStr(ex)), ("event", Value(flowEvent).toJson))
        val retryCount = retryFlowEvents.getOrDefault(flowEvent.getEventId, 0)
        if (retryCount >= maxRetryCount) {
          //反复尝试后依旧无法正常执行的flowEvent，会留在mailbox中
          error(("msg", "maximum retry limit is reached"), ("event", Value(flowEvent).toJson))
          ignoreFailEvent(flowEvent)
        } else {
          //重试等待100ms ~ 1000ms
          Thread.sleep(retryCount * 500)
          //内存里记录需要重试的flowEvent，以及重试次数，每次重试等待更长时间，直到重试次数上限
          //如果执行失败，继续重试执行
          retryFlowEvents.put(flowEvent.getEventId, retryCount + 1)
          exec(flowEvent)
          info(("msg", "retry executor flow"), ("source", sourceName), ("retryCount", retryCount),
            ("eventId", flowEvent.getEventId))
        }
    }
  }

  private def ignoreFailEvent(flowEvent: FlowEvent): Unit = {
    val instance = FlowInstance.newBuilder().setFlowId("unknown").setEvent(flowEvent)
    val fib = storeService.commitEvent(sourceName, instance)
    if (fib == null) {
      error(("msg", "commit event error"), ("instance", Value(instance).toJson))
    }
  }

  override def putEventSync(flowEvent: FlowEvent): FlowInstance = {
    var result: FlowInstance = null
    val acquireResult = rateLimiter.tryAcquire(backPressTimeout, TimeUnit.SECONDS)
    if (acquireResult) {
      try {
        storeService.addEvent(sourceName, flowEvent.toBuilder)
        val f = flowExecutor.execute(flowEvent)
        val executeTimeout = Try(params.getInt("execute-timeout")).getOrElse(10)
        val flowInstance = Await.result(f, executeTimeout seconds)
        if (commitCheck(flowEvent)) {
          val fib = storeService.commitEvent(sourceName, flowInstance.toBuilder)
          if (fib == null) {
            error(("msg", "commit check event error"), ("instance", Value(flowInstance).toJson))
            throw EventProcessException(CommitError)
          } else {
            debug(("msg", "event execute success"),
              ("instance", Value(fib).toJson), ("duration", s"${fib.getEndTime - fib.getStartTime} ms"))
            info(("msg", "event execute success"), ("source", sourceName),
              ("duration", s"${fib.getEndTime - fib.getStartTime} ms"))
            // 打印ODS分析日志
            ctx.odsLogger.log(flowInstance)
            result = fib.asInstanceOf[FlowInstance]
          }
        } else {
          throw EventProcessException(CheckError)
        }
      } catch {
        case ex: TimeoutException =>
          error(("msg", "time out"), ("ex", printStackTraceStr(ex)))
          throw EventProcessException(TimeOutError, ex)
        case ex: Throwable =>
          error(("msg", "executor error"), ("ex", printStackTraceStr(ex)))
          throw EventProcessException(ExecuteError, ex)
      }
    } else {
      //back press error
      throw EventProcessException(BackPressError)
    }
    result
  }
}
