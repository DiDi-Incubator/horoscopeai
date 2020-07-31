/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import java.util.concurrent._

import com.didichuxing.horoscope.core.FlowRuntimeMessage.{FlowEvent, FlowInstance}
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.service.resource.Participant
import com.didichuxing.horoscope.util.Logging
import com.typesafe.config.Config

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.util.{Failure, Success}

abstract class RPCEventBus(sourceName: String, flowName: String, params: Config)
                          (implicit ctx: ApplicationContext)
  extends AbstractEventBus(sourceName, flowName, params) with Logging {

  implicit val ec = ctx.sourceExecutionContext.getExecutionContext()
  protected val resourceManager = ctx.resourceManager
  private val eventRouter = new DefaultEventRouter()

  override def start(): Unit = {
    super.start()
    //启动rpc客户端
    resourceManager.registerRPCClient(sourceName, params)
  }

  override def stop(): Unit = {
    resourceManager.removeRPCClient(sourceName, params)
    super.stop()
  }

  //并发的请求其他机器，获取处理结果
  override def doProcess(processEvents: List[FlowEvent]): List[FlowEvent] = {
    //消息路由
    val routingEvents = eventRouter.routing(processEvents)
    debug(("msg", "routingEvents"), ("count", routingEvents.size))
    //async rpc，successFlowEvents的顺序和flowEvents保持一致
    val successFlowEvents = new Array[FlowEvent](processEvents.size)
    val cdl = new CountDownLatch(routingEvents.size)
    val beginTime = System.currentTimeMillis()
    for (routingEvent <- routingEvents) {
      val participant = routingEvent._1
      val events = routingEvent._2
      val localPart = resourceManager.local()
      if (localPart == participant) {
        //local callback
        localEvents(events.toList).onComplete {
          case Success(list) =>
            debug(("msg", "event bus local callback"))
            try {
              list.foreach(seqEvent => {
                if (seqEvent.flowEvent != null) {
                  successFlowEvents(seqEvent.seq) = seqEvent.flowEvent
                }
              })
            } finally {
              cdl.countDown()
            }
          case Failure(exception) =>
            cdl.countDown()
        }
      } else {
        //rpc callback
        rpcEvents(participant, events.toList).onComplete {
          case Success(list) =>
            debug(("msg", "event bus remote callback"))
            try {
              list.foreach(seqEvent => {
                if (seqEvent.flowEvent != null) {
                  successFlowEvents(seqEvent.seq) = seqEvent.flowEvent
                }
              })
            } finally {
              cdl.countDown()
            }
          case Failure(exception) =>
            error(("msg", "rpc error"), ("source", sourceName), ("ex", exception.getCause))
            cdl.countDown()
        }
      }
    }
    // 可能是服务器异常，无响应（暂定一个15分钟超时）通常这个异常出现证明系统有不可恢复的问题，需要报警，人工介入
    if (!cdl.await(15, TimeUnit.MINUTES)) {
      error(("msg", "rpc timeout"), ("source", sourceName))
    }
    val endTime = System.currentTimeMillis()
    info(("msg", "event bus process time"), ("source", sourceName), ("proc_time", s"${endTime - beginTime}ms"))
    successFlowEvents.toList
  }

  private def localEvents(seqFlowEvents: List[SeqFlowEvent]): Future[List[SeqFlowEvent]] = {
    Future[List[SeqFlowEvent]] {
      val flowEvents = seqFlowEvents.map(v => v.flowEvent)
      val events = eventProcessor.putEvent(flowEvents)
      val results = ListBuffer[SeqFlowEvent]()
      for ((seq, i) <- seqFlowEvents.zipWithIndex) {
        results.append(SeqFlowEvent(seq.seq, events(i)))
      }
      results.toList
    }
  }

  def rpcEvents(participant: Participant, flowEvents: List[SeqFlowEvent]): Future[List[SeqFlowEvent]]

  override def doProcessSync(flowEvent: FlowEvent): FlowInstance = {
    //消息路由
    val routingEvents = eventRouter.routing(List[FlowEvent](flowEvent))
    debug(("msg", "routingEvents"), ("count", routingEvents.size))
    val participants = routingEvents.keys.toList
    val participant = participants(0)
    val event = routingEvents(participant)(0).flowEvent
    val localPart = resourceManager.local()
    if (localPart == participant) {
      debug(("msg", "event bus local request"))
      eventProcessor.putEventSync(event)
    } else {
      debug(("msg", "event bus remote request"))
      syncRpcEvents(participant, event)
    }
  }

  def syncRpcEvents(participant: Participant, flowEvent: FlowEvent): FlowInstance

}
