/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.{Callable, Future, TimeUnit}

import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowEvent
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.service.resource.ResourceManager
import com.didichuxing.horoscope.util.Utils._
import com.didichuxing.horoscope.util.{Logging, Utils}
import com.typesafe.config.Config

/**
 * 集群版本的消息处理实现
 *
 * @param params
 * @param sourceName
 */
abstract class RPCEventProcessor(sourceName: String, params: Config)(implicit ctx: ApplicationContext)
  extends DefaultEventProcessor(sourceName, params) with Logging {

  val resourceManager: ResourceManager = ctx.resourceManager
  val slotCount: Int = Utils.getClusterSlotCount(ctx.config)
  val restorerFuture = new AtomicReference[Future[Unit]]()

  def startRPCServer()

  def stopRPCServer()

  override def start(): Unit = {
    super.start()
    // 启动泛源时重新恢复执行mailbox中的数据
    recovery()
    startRPCServer()
    if (routineRecovery) {
      this.synchronized {
        restorerFuture.set(ec.submit(new RecoveryExecutor(sourceName, recoveryIntervalInSecond)))
      }
    }
  }

  class RecoveryExecutor(source: String, recoveryInterval: Int) extends Callable[Unit] {
    override def call(): Unit = {
      val status = new AtomicBoolean(false)
      if (status.compareAndSet(false, true)) {
        while(status.get() && !Thread.currentThread().isInterrupted) {
          try {
            recovery(true)
            Thread.sleep(recoveryInterval * 1000)
          } catch {
            case iex: InterruptedException =>
              val interrupted = status.compareAndSet(true, false)
              info(("msg", "source restorer interrupted"), ("source", source), ("interrupted", interrupted))
            case throwable: Throwable =>
              error(("msg", "source restorer error"), ("source", source), ("ex", printStackTraceStr(throwable)))
          }
        }
      }
    }
  }

  // isRoutine: false, 泛源启动时recovery, mailbox所有数据全部执行
  // isRoutine: true,  泛源启动后每隔一段时间例行recovery, 仅处理据现在半小时之前的
  private def recovery(isRoutine: Boolean = false): Unit = {
    debug(("msg", "source recovery begin"), ("source", sourceName))
    //recover mailbox
    val startTime = System.currentTimeMillis()
    val slotRange = resourceManager.getSlotRange(resourceManager.local())
    if (slotRange.isDefined) {
      val range = slotRange.get
      debug(("msg", "source recovery range"), ("source", sourceName), ("range", range))
      var events = storeService.getEventsBySource(sourceName, range.begin, range.end).map(_.asInstanceOf[FlowEvent])
      // 例行recovery时, 处理距现在1h之前的events, 每次最多处理100条
      if (isRoutine) {
        events = events.filter(_.getScheduledTimestamp < startTime - 3600 * 1000).take(100)
      }
      val total = events.size
      debug(("msg", "source recovery events count"), ("source", sourceName), ("count", total))
      if (total > 0) {
        //保证启动的时候recovery的event数量不会被反压拦住
          info(("msg", "source recovery count"), ("source", sourceName), ("count", total))
        for (event <- events) {
          val acquireResult = rateLimiter.tryAcquire(1, backPressTimeout, TimeUnit.SECONDS)
          if (acquireResult) {
            exec(event)
          }
        }
        info(("msg", "source recovery finished"), ("source", sourceName),
          ("count", total), ("process_time", s"${System.currentTimeMillis() - startTime} ms"))
      }
    } else {
      error(("msg", "source recovery slot range not found"), ("source", sourceName))
    }
  }

  override def stop(): Unit = {
    stopRPCServer()
      this.synchronized {
        try {
          val future = restorerFuture.get()
          if (future == null) {
            info(("msg", "source recovery is not started"), ("source", sourceName))
          } else if (future.isDone || future.isCancelled) {
            info(("msg", "source recovery has stopped"), ("source", sourceName))
          } else {
            val cancelResult = future.cancel(true)
            info(("msg", "source recovery cancel"), ("source", sourceName), ("cancel_result", cancelResult))
          }
        } catch {
          case ex: Throwable =>
            error(("msg", "source recovery stop error"), ("source", sourceName), ("ex", ex.getMessage))
        }
      }
  }

  override def commitCheck(flowEvent: FlowEvent): Boolean = {
    val localServer = resourceManager.local()
    val slotRange = resourceManager.getSlotRange(localServer)
    if (slotRange.isDefined) {
      val range = slotRange.get
      val traceId = flowEvent.getTraceId
      val slot = getSlot(traceId, slotCount)
      if (slot >= range.begin && slot < range.end) {
        true
      } else {
        error(("msg", "commit slot range error"), ("source", sourceName), ("slot", slot),
          ("range", range), ("localServer", localServer))
        false
      }
    } else {
      error(("msg", "commit slot range not found"), ("source", sourceName))
      false
    }
  }

}
