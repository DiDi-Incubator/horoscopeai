/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import java.util.concurrent.TimeUnit

import com.didichuxing.horoscope.core.EventBusRPC.ResponseHeader.ERRNO
import com.didichuxing.horoscope.core.EventBusRPC
import com.didichuxing.horoscope.core.FlowRuntimeMessage.{FlowEvent, FlowInstance}
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.service.resource.Participant
import com.didichuxing.horoscope.util.{Logging, SystemLog}
import com.google.common.util.concurrent.{FutureCallback, Futures}
import com.typesafe.config.Config

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Future, Promise}
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * 基于GRPC的集群版本实现
 *
 * @param sourceName
 * @param flowName
 * @param params
 */
class GRPCEventBus(sourceName: String, flowName: String, params: Config)
                  (implicit ctx: ApplicationContext) extends RPCEventBus(sourceName, flowName, params) with Logging {

  override def newEventProcessor(): EventProcessor = {
    new GRPCEventProcessor(sourceName, params)
  }

  //EventBusRPC.EventReply
  def rpcEvents(participant: Participant, flowEvents: List[SeqFlowEvent]): Future[List[SeqFlowEvent]] = {
    val seqEventMap = mutable.Map[String, FlowEvent]()
    val request = EventBusRPC.EventRequestSimple.newBuilder().setTraceInfo(SystemLog.get())
    flowEvents.foreach(seqFlowEvent => {
      val flowEvent = seqFlowEvent.flowEvent
      request.putSeqEvents(seqFlowEvent.seq, flowEvent)
      seqEventMap.put(flowEvent.getEventId, flowEvent)
    })
    val pid = participant.getParticipantId()
    val serviceClient = resourceManager.getRPCClient(pid, sourceName, params)
    if (serviceClient.isDefined) {
      val gepc = serviceClient.get.asInstanceOf[GRPCEventProcessorClient]
      val stub = gepc.stub
      val lf = stub.putEventSimple(request.build())
      val p = Promise[List[SeqFlowEvent]]()
      Futures.addCallback(lf, new FutureCallback[EventBusRPC.EventReplySimple] {
        override def onSuccess(result: EventBusRPC.EventReplySimple): Unit = {
          SystemLog.set(result.getHeader.getTraceInfo)
          val seqList = ListBuffer[SeqFlowEvent]()
          val errno = result.getHeader.getErrno
          if (errno == ERRNO.OK) {
            val seqEventIds = result.getSeqEventIdsMap
            if(seqEventIds != null) {
              seqEventIds.foreach {
                case (seq, eventId) =>
                  if (eventId != null && seqEventMap.get(eventId).isDefined) {
                    seqList.add(SeqFlowEvent(seq, seqEventMap.get(eventId).get))
                  }
              }
            }
          } else {
            error(("msg", "event bus rpc callback"), ("code", errno))
          }
          p.success(seqList.toList)
        }

        override def onFailure(t: Throwable): Unit = {
          p.failure(t)
        }
      })
      p.future
    } else {
      Future.failed(new Exception(s"participant rpc client:${pid} not found"))
    }
  }

  override def syncRpcEvents(participant: Participant, flowEvent: FlowEvent): FlowInstance = {
    val pid = participant.getParticipantId()
    val serviceClient = resourceManager.getRPCClient(pid, sourceName, params)
    if (serviceClient.isDefined) {
      val gepc = serviceClient.get.asInstanceOf[GRPCEventProcessorClient]
      val stub = gepc.stub
      val request = EventBusRPC.SingleEventRequest.newBuilder().setTraceInfo(SystemLog.get())
      request.setEvent(flowEvent)
      try {
        //可能反压，等待一个最大的15分钟时间
        val result = stub.putEventSync(request.build()).get(15, TimeUnit.MINUTES)
        val header = result.getHeader
        SystemLog.set(header.getTraceInfo)
        val code = header.getErrno
        if (code == ERRNO.OK) {
          result.getInstance()
        } else {
          code match {
            case ERRNO.BACKPRESS =>
              throw EventProcessException(EventProcessErrorCode.BackPressError)
            case ERRNO.CHECK =>
              throw EventProcessException(EventProcessErrorCode.CheckError)
            case ERRNO.COMMIT =>
              throw EventProcessException(EventProcessErrorCode.CommitError)
            case ERRNO.EXECUTE =>
              throw EventProcessException(EventProcessErrorCode.ExecuteError)
            case ERRNO.TIMEOUT =>
              throw EventProcessException(EventProcessErrorCode.TimeOutError)
            case _ =>
              throw EventProcessException(EventProcessErrorCode.UnknownError)
          }
        }
      } catch {
        case ex: Exception =>
          error(("msg", "syncRpcEvents error"), ("ex", ex.getCause))
          throw EventProcessException(EventProcessErrorCode.RpcClientError, ex)
      }
    } else {
      throw EventProcessException(EventProcessErrorCode.RpcClientError)
    }
  }
}
