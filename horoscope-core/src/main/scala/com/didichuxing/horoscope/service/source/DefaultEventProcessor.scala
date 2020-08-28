/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import java.util.concurrent.{ConcurrentHashMap, Semaphore, TimeUnit}

import com.didichuxing.horoscope.core.FlowRuntimeMessage.{FlowEvent, FlowInstance}
import com.didichuxing.horoscope.runtime.convert.ValueTypeAdapter
import com.didichuxing.horoscope.runtime.{IgnoredException, Value}
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.service.source.EventProcessErrorCode._
import com.didichuxing.horoscope.util.Utils._
import com.didichuxing.horoscope.util.{Logging, PublicLog, Utils}
import com.google.gson.{Gson, GsonBuilder}
import com.google.protobuf.util.JsonFormat
import com.typesafe.config.Config

import scala.collection.mutable.ListBuffer
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class DefaultEventProcessor(sourceName: String, params: Config)
                           (implicit ctx: ApplicationContext) extends EventProcessor with Logging {

  implicit val ec = ctx.sourceExecutionContext.getExecutionContext()
  val storeService = ctx.traceStore
  val flowExecutor = ctx.flowExecutor
  val scheduler = ctx.scheduler
  //反压超时时间，默认10秒
  val backPressTimeout = configIntOrDefault(params, "backpress.timeout", 10)
  //反压
  var backPress: Semaphore = _
  //pb转json
  val jsonPrinter = JsonFormat.printer.omittingInsignificantWhitespace()
  implicit val gson: Gson = new GsonBuilder()
    .registerTypeHierarchyAdapter(classOf[Value], new ValueTypeAdapter)
    .create()
  //retry  eventId:retryCount
  val retryFlowEvents = new ConcurrentHashMap[String, Integer]()
  //最大重试次数
  val maxRetryCount = configIntOrDefault(params, "exec.retry", 10)

  override def start(): Unit = {
    backPress = new Semaphore(Try(params.getInt("backpress.permits")).getOrElse(100), true)
  }

  override def stop(): Unit = {}

  override def putEvent(events: List[FlowEvent]): List[FlowEvent] = {
    info(("msg", "available backpress"), ("source", sourceName), ("count", backPress.availablePermits()))
    //获取反压令牌
    val eventSize = events.size
    val backPressResult = backPress.tryAcquire(eventSize, backPressTimeout, TimeUnit.SECONDS)
    if (backPressResult) {
      var successMailbox = false
      try {
        //批量放入mailbox
        val beginTime = System.currentTimeMillis()
        storeService.addEvents(sourceName, events.map(v => v.toBuilder))
        successMailbox = true
        val endTime = System.currentTimeMillis()
        info(("msg", "add mailbox process time"), ("source", sourceName), ("proc_time", s"${endTime - beginTime} ms"))
      } catch {
        case ex: Exception =>
          error(("msg", "add event to mailbox error"), ("ex", ex.getMessage))
          //放入mailbox异常，释放反压令牌
          backPress.release(eventSize)
      }
      if (successMailbox) {
        val results = ListBuffer[FlowEvent]()
        //执行event
        for (flowEvent <- events) {
          exec(flowEvent)
          results.append(flowEvent)
        }
        //返回成功的event
        results.toList
      } else {
        error(("msg", "add mailbox error"), ("available", backPress.availablePermits()))
        new Array[FlowEvent](eventSize).toList
      }
    } else {
      error(("msg", "add mailbox error backpress timeout"), ("available", backPress.availablePermits()))
      new Array[FlowEvent](eventSize).toList
    }
  }

  def exec(flowEvent: FlowEvent) {
    //flow execute callback
    flowExecutor.execute(flowEvent).onComplete {
      case Success(flowInstance) =>
        onSuccess(flowInstance)
      case Failure(exception) =>
        //flowEvent闭包
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
          backPress.release()
        } else {
          debug(("msg", "event execute success"),
            ("instance", Value(fib).toJson), ("duration", s"${fib.getEndTime - fib.getStartTime} ms"))
          info(("msg", "event execute success"), ("source", sourceName),
            ("duration", s"${fib.getEndTime - fib.getStartTime} ms"))
          // 打印ODS分析日志
          ctx.odsLogger.log(flowInstance)
          //如果需要goto，发送给Scheduler
          if (fib.hasGoto) {
            val gotoEvent = fib.getGoto
            if (gotoEvent.hasScheduledTimestamp && gotoEvent.getScheduledTimestamp > 0) {
              scheduler.goto(fib.getGoto)
              backPress.release()
            } else {
              //继续执行，不释放反压令牌
              exec(gotoEvent)
            }
          } else {
            //不需要goto
            backPress.release()
          }
        }
      } else {
        //warn：这里会直接丢弃，也就是可能出现mailbox里的消息未处理完成。需要重启服务后recovery重新执行。
        //对于source消息，是否可以采用每隔一段时间扫描mailbox，发现有长时间未被执行的消息重新执行
        //对于scheduler消息，是否可以触发一次集群整体的scheduler recovery
        backPress.release()
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
        warn(("msg", "executor event with ignored exception"), ("ex", ex.toString),
          ("event", Value(flowEvent).toJson))
        ex.printStackTrace()
        val instance = FlowInstance.newBuilder().setFlowId("unknown").setEvent(flowEvent)
        val fib = storeService.commitEvent(sourceName, instance)
        if (fib == null) {
          error(("msg", "commit event error"), ("instance", Value(instance).toJson))
        }
        backPress.release()

      case err: ExecutionException if err.getCause.isInstanceOf[Error] =>
        //Error异常不重试，如：不属于该分区
        error(("msg", "executor event error"), ("ex", err.toString),
          ("event", Value(flowEvent).toJson))
        err.printStackTrace()
        backPress.release()

      case ex: Throwable =>
        error(("msg", "executor event error"), ("ex", ex.toString), ("event", Value(flowEvent).toJson))
        ex.printStackTrace()
        val retryCount = retryFlowEvents.getOrDefault(flowEvent.getEventId, 1)
        if (retryCount >= maxRetryCount) {
          //反复尝试后依旧无法正常执行的flowEvent，会留在mailbox中
          error(("msg", "maximum retry limit is reached"), ("event", Value(flowEvent).toJson))
          backPress.release()
        } else {
          //重试等待100ms ~ 1000ms
          Thread.sleep(retryCount * 100)
          //内存里记录需要重试的flowEvent，以及重试次数，每次重试等待更长时间，直到重试次数上限
          //如果执行失败，继续重试执行
          retryFlowEvents.put(flowEvent.getEventId, retryCount + 1)
          exec(flowEvent)
          info(("msg", "retry executor flow"), ("source", sourceName), ("retryCount", retryCount),
            ("eventId", flowEvent.getEventId))
        }
    }
  }

  override def putEventSync(flowEvent: FlowEvent): FlowInstance = {
    var result: FlowInstance = null
    val backPressResult = backPress.tryAcquire(backPressTimeout, TimeUnit.SECONDS)
    if (backPressResult) {
      try {
        storeService.addEvent(sourceName, flowEvent.toBuilder)
        val f = flowExecutor.execute(flowEvent)
        val executeTimeout = Try(params.getInt("execute-timeout")).getOrElse(3)
        val flowInstance = Await.result(f, executeTimeout seconds)
        if (commitCheck(flowEvent)) {
          val fib = storeService.commitEvent(sourceName, flowInstance.toBuilder)
          if (fib == null) {
            error(("msg", "commit check event error"), ("instance", Value(flowInstance).toJson))
            backPress.release()
            throw EventProcessException(CommitError)
          } else {
            debug(("msg", "event execute success"),
              ("instance", Value(fib).toJson), ("duration", s"${fib.getEndTime - fib.getStartTime} ms"))
            info(("msg", "event execute success"), ("source", sourceName),
              ("duration", s"${fib.getEndTime - fib.getStartTime} ms"))
            // 打印ODS分析日志
            ctx.odsLogger.log(flowInstance)
            result = fib.asInstanceOf[FlowInstance]
            if (fib.hasGoto) {
              val gotoEvent = fib.getGoto
              if (gotoEvent.hasScheduledTimestamp && gotoEvent.getScheduledTimestamp > 0) {
                scheduler.goto(fib.getGoto)
                backPress.release()
              } else {
                //继续执行，不释放反压令牌
                result = putEventSync(gotoEvent)
              }
            } else {
              //不需要goto
              backPress.release()
            }
          }
        } else {
          backPress.release()
          throw EventProcessException(CheckError)
        }
      } catch {
        case ex: TimeoutException =>
          ex.printStackTrace()
          error(("msg", "time out"), ("ex", ex.getCause))
          backPress.release()
          throw EventProcessException(TimeOutError)
        case ex: Throwable =>
          ex.printStackTrace()
          error(("msg", "executor error"), ("ex", ex.getCause))
          backPress.release()
          throw EventProcessException(ExecuteError)
      }
    } else {
      //back press error
      throw EventProcessException(BackPressError)
    }
    result
  }
}
