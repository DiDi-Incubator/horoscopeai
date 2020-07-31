/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import java.nio.ByteBuffer

import com.didichuxing.horoscope.core.FlowRuntimeMessage.{FlowEvent, FlowInstance}
import com.didichuxing.horoscope.core.event_busConstants._
import com.didichuxing.horoscope.core._
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.service.resource.Participant
import com.didichuxing.horoscope.util.{Logging, SystemLog}
import com.typesafe.config.Config
import org.apache.thrift.transport.TTransportException

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Future, Promise}

/**
 * 基于Thrift的集群版本实现
 *
 * @param sourceName
 * @param flowName
 * @param params
 */
class ThriftEventBus(sourceName: String, flowName: String, params: Config)
                    (implicit ctx: ApplicationContext) extends RPCEventBus(sourceName, flowName, params) with Logging {

  override def newEventProcessor(): EventProcessor = {
    new ThriftEventProcessor(sourceName, params)
  }

  def rpcEvents(participant: Participant, flowEvents: List[SeqFlowEvent]): Future[List[SeqFlowEvent]] = {
    val eventMap = mutable.Map[String, FlowEvent]()
    val request = new EventRequest()
    request.setTraceInfo(SystemLog.get())
    flowEvents.foreach(v => {
      eventMap.put(v.flowEvent.getEventId, v.flowEvent)
      request.putToSeqEvents(v.seq, ByteBuffer.wrap(v.flowEvent.toByteArray))
    })
    val pid = participant.getParticipantId()
    val serviceClient = resourceManager.getRPCClient(pid, sourceName, params)
    if (serviceClient.isDefined) {
      val p = Promise[List[SeqFlowEvent]]()
      Future {
        val tepc = serviceClient.get.asInstanceOf[ThriftEventProcessorClient]
        val stub = tepc.stub
        try {
          val startTime = System.currentTimeMillis()
          val result = stub.putEvent(request)
          info(("msg", "thrift rpc process time"), ("proc_time", s"${System.currentTimeMillis() - startTime}ms"))
          SystemLog.set(result.getHeader.getTraceInfo)
          val errno = result.getHeader.getErrno
          if (errno == ERRNO_OK) {
            val seqList = ListBuffer[SeqFlowEvent]()
            val seqEventIds = result.getSeqEventIds
            if(seqEventIds != null) {
              seqEventIds.foreach {
                case (idx, eventId) =>
                  if (eventId != null && eventMap.get(eventId).isDefined) {
                    seqList.append(SeqFlowEvent(idx, eventMap.get(eventId).get))
                  }
              }
            }
            p.success(seqList.toList)
          } else {
            error(("msg", "event bus rpc callback"), ("code", errno))
            p.failure(new Exception(s"rpc error code = $errno"))
          }
        } catch {
          case ex: TTransportException =>
            error(("msg", "thrift transport error"), ("type", ex.getType), ("ex", ex.getCause))
            //异常关闭连接，重连
            tepc.reconnect(stub)
            p.failure(ex)
          case ex: Exception =>
            error(("msg", "rpcEvents error"), ("ex", ex.getCause))
            p.failure(ex)
        } finally {
          tepc.release(stub)
        }
      }
      p.future
    } else {
      Future.failed(new Exception(s"participant rpc client:${pid} not found"))
    }
  }

  override def syncRpcEvents(participant: Participant, flowEvent: FlowEvent): FlowInstance = {
    val pid = participant.getParticipantId()
    val serviceClient = resourceManager.getRPCClient(pid, sourceName, params)
    if (serviceClient.isDefined) {
      val tepc = serviceClient.get.asInstanceOf[ThriftEventProcessorClient]
      val stub = tepc.stub
      val request = new SingleEventRequest()
      request.setTraceInfo(SystemLog.get())
      request.setEvent(ByteBuffer.wrap(flowEvent.toByteArray))
      try {
        val result = stub.putEventSync(request)
        val header = result.getHeader
        val logId = header.getTraceInfo
        SystemLog.set(logId)
        val code = header.getErrno
        if (code == ERRNO_OK) {
          FlowInstance.parseFrom(result.getInstance())
        } else {
          code match {
            case ERRNO_BACKPRESS =>
              throw EventProcessException(EventProcessErrorCode.BackPressError)
            case ERRNO_CHECK =>
              throw EventProcessException(EventProcessErrorCode.CheckError)
            case ERRNO_COMMIT =>
              throw EventProcessException(EventProcessErrorCode.CommitError)
            case ERRNO_EXECUTE =>
              throw EventProcessException(EventProcessErrorCode.ExecuteError)
            case ERRNO_TIMEOUT =>
              throw EventProcessException(EventProcessErrorCode.TimeOutError)
            case _ =>
              throw EventProcessException(EventProcessErrorCode.UnknownError)
          }
        }
      } catch {
        case ex: TTransportException =>
          error(("msg", "thrift transport error"), ("type", ex.getType), ("ex", ex.getCause))
          //异常关闭连接，重连
          tepc.reconnect(stub)
          throw EventProcessException(EventProcessErrorCode.RpcClientError)
        case ex: Exception =>
          ex.printStackTrace()
          throw EventProcessException(EventProcessErrorCode.RpcClientError)
      } finally {
        tepc.release(stub)
      }
    } else {
      throw EventProcessException(EventProcessErrorCode.RpcClientError)
    }
  }
}
