/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import java.util.concurrent.Semaphore

import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowEvent
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.util.Utils._
import com.didichuxing.horoscope.util.{Logging, Utils}
import com.typesafe.config.Config

/**
 * 集群版本的消息处理实现，继承Semaphore的反压实现
 *
 * @param params
 * @param sourceName
 */
abstract class RPCEventProcessor(sourceName: String, params: Config)(implicit ctx: ApplicationContext)
  extends DefaultEventProcessor(sourceName, params) with Logging {

  val resourceManager = ctx.resourceManager
  val slotCount = Utils.getClusterSlotCount(ctx.config)

  def startRPCServer()

  def stopRPCServer()

  override def start(): Unit = {
    super.start()
    recovery()
    startRPCServer()
  }

  private def recovery(): Unit = {
    debug(("msg", "source recovery begin"), ("source", sourceName))
    //recover mailbox
    val slotRange = resourceManager.getSlotRange(resourceManager.local())
    if (slotRange.isDefined) {
      val range = slotRange.get
      debug(("msg", "source recovery range"), ("source", sourceName), ("range", range))
      val events = storeService.getEventsBySource(sourceName, range.begin, range.end)
        .map(e => e.asInstanceOf[FlowEvent])
      debug(("msg", "source recovery events count"), ("source", sourceName), ("count", events.size))
      val total = events.size
      if (total > 0) {
        //保证启动的时候recovery的event数量不会被反压拦住
        val max = params.getInt("backpress.permits")
        if (total >= max) {
          warn(("msg", "source recovery count more than backpress"), ("source", sourceName),
            ("count", total), ("backpress", max))
          backPress = new Semaphore(max - total, true)
        } else {
          backPress.acquire(total)
        }
        for (event <- events) {
          exec(event)
        }
      }
    } else {
      error(("msg", "source recovey slot range not found"), ("source", sourceName))
    }
  }

  override def stop(): Unit = {
    stopRPCServer()
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
