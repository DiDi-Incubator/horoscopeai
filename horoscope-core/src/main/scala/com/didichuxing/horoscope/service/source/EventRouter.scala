/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowEvent
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.service.resource.{Participant, ResourceManager}
import com.didichuxing.horoscope.util.{Logging, Utils}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * 为了保证顺序，需要记录每个event再rpc请求前的序号
 *
 * @param seq       序号
 * @param flowEvent event
 */
case class SeqFlowEvent(seq: Int, flowEvent: FlowEvent)

/**
 * 对应集群办法，该方法返回分区路由后的信息
 */
trait EventRouter {

  //partitionId:events
  def routing(events: List[FlowEvent]): Map[Participant, ListBuffer[SeqFlowEvent]]

}

class DefaultEventRouter()(implicit ctx: ApplicationContext) extends EventRouter with Logging {

  val resourceManager = ctx.resourceManager
  val slotCount = Utils.getClusterSlotCount(ctx.config)

  override def routing(events: List[FlowEvent]): Map[Participant, ListBuffer[SeqFlowEvent]] = {

    val result = mutable.Map[Participant, ListBuffer[SeqFlowEvent]]()
    val participants = resourceManager.getParticipants()
    for(participant <- participants) {
      result.put(participant, ListBuffer[SeqFlowEvent]())
    }
    for ((event, i) <- events.zipWithIndex) {
      val traceId = event.getTraceId
      val slotPart = hash(participants, traceId)
      if(slotPart.isDefined) {
        val participant = slotPart.get
        debug(("msg", "route result"), ("traceId", traceId), ("participant", participant))
        val listBuffer = result.get(participant).get
        listBuffer.append(SeqFlowEvent(i, event))
      } else {
        error(("msg", "event router not found slot"), ("traceId", event.getTraceId))
        //兜底，保证必须有一个服务器处理消息
        if(participants.size > 0) {
          val participant = participants(0)
          val listBuffer = result.get(participant).get
          listBuffer.append(SeqFlowEvent(i, event))
        } else {
          error(("msg", "event router not default participant"), ("traceId", event.getTraceId))
        }
      }
    }
    result.filter(p => p._2.size > 0).toMap
  }

  /**
   * 计算traceId归属的服务器
   * @param participants
   * @param traceId
   * @return
   */
  private def hash(participants: List[Participant], traceId: String): Option[Participant] = {
    val slotPart = participants.filter(participant => {
      val slotRange = resourceManager.getSlotRange(participant)
      if (slotRange.isEmpty) {
        error(("msg", "event router slot range not found"), ("traceId", traceId))
        false
      } else {
        val slot = Utils.getSlot(traceId, slotCount)
        val range = slotRange.get
        if (slot >= range.begin && slot < range.end) {
          true
        } else {
          false
        }
      }
    })
    if (slotPart.size == 1) {
      Some(slotPart(0))
    } else {
      None
    }
  }
}
