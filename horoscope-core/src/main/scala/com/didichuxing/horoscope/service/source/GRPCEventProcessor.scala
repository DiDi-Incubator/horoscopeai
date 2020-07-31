/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import java.util

import com.didichuxing.horoscope.core.EventBusRPC.{ResponseHeader, SingleEventReply}
import com.didichuxing.horoscope.core.EventBusRPC.ResponseHeader.ERRNO
import com.didichuxing.horoscope.core.FlowRuntimeMessage.{FlowEvent}
import com.didichuxing.horoscope.core.{EventBusRPC, EventProcessorServiceGrpc}
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.util.Utils._
import com.didichuxing.horoscope.util.{Logging, SystemLog}
import com.typesafe.config.Config
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import io.grpc.Server
import io.grpc.stub.StreamObserver

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
 * 集群版本的消息处理实现，继承Semaphore的反压实现
 *
 * @param params
 * @param sourceName
 */
class GRPCEventProcessor(sourceName: String, params: Config)(implicit ctx: ApplicationContext)
  extends RPCEventProcessor(sourceName, params) with Logging {

  var server: Server = _

  override def startRPCServer(): Unit = {
    val port = params.getInt("rpc.port")
    //grpc flow control window, default 100MB
    val window = configIntOrDefault(params, "rpc.window", 102400) * 1024
    server = NettyServerBuilder.forPort(port).flowControlWindow(window).executor(ec)
      .addService(new EventProcessorServiceImpl).build.start
    debug(("msg", "source grpc processor init complete"), ("source", sourceName), ("port", port), ("server", server))
  }

  override def stopRPCServer(): Unit = {
    if (server != null) {
      server.shutdown
    }
  }

  //服务端实现
  class EventProcessorServiceImpl extends EventProcessorServiceGrpc.EventProcessorServiceImplBase with Logging {

    override def putEvent(request: EventBusRPC.EventRequest,
                          responseObserver: StreamObserver[EventBusRPC.EventReply]): Unit = {

      val beginTime = System.currentTimeMillis()
      SystemLog.set(request.getTraceInfo)
      debug(("msg", "remote receive events"), ("server", resourceManager.local().getParticipantId()))
      //带序号的events
      val seqFlowEvents = request.getEventsList
      //不带序号的events
      val flowEvents = ListBuffer[FlowEvent]()
      for (seq <- seqFlowEvents) {
        flowEvents.append(seq.getEvent)
      }
      //成功执行的events
      val successEvents = GRPCEventProcessor.this.putEvent(flowEvents.toList)
      //返回
      val headerBuilder = ResponseHeader.newBuilder().setErrno(ERRNO.OK).setTraceInfo(SystemLog.get())
      val replyBuilder = EventBusRPC.EventReply.newBuilder
      if (successEvents.size != flowEvents.size) {
        //必须保证返回和顺序和数量一致
        replyBuilder.setHeader(headerBuilder.setErrno(ERRNO.INVAL).build())
      } else {
        //返回成功执行的带序号的events
        val successSeqFlowEvents = new util.ArrayList[EventBusRPC.SeqFlowEvent]()
        for ((event, i) <- successEvents.zipWithIndex) {
          val seqFlowEvent = seqFlowEvents.get(i)
          if (event == null) {
            //失败返回null，也回传给client，只包含idx
            val seqEvent = EventBusRPC.SeqFlowEvent.newBuilder().setIdx(seqFlowEvent.getIdx).build()
            successSeqFlowEvents.append(seqEvent)
          } else {
            successSeqFlowEvents.append(seqFlowEvent)
          }
        }
        replyBuilder.setHeader(headerBuilder.build())
        replyBuilder.addAllSuccessEvents(successSeqFlowEvents)
      }
      responseObserver.onNext(replyBuilder.build())
      responseObserver.onCompleted()
      val endTime = System.currentTimeMillis()
      debug(("msg", "remote process events complete"), ("proc_time", s"${endTime - beginTime}ms"))
    }

    override def putEventSimple(request: EventBusRPC.EventRequestSimple,
                                responseObserver: StreamObserver[EventBusRPC.EventReplySimple]): Unit = {

      val beginTime = System.currentTimeMillis()
      SystemLog.set(request.getTraceInfo)
      debug(("msg", "remote receive events"), ("server", resourceManager.local().getParticipantId()))
      //带序号的events
      val seqEventsMap = request.getSeqEventsMap
      val seqFlowEvents = ListBuffer[SeqFlowEvent]()
      for ((seq, event) <- seqEventsMap) {
        seqFlowEvents.append(SeqFlowEvent(seq, event))
      }
      //不带序号的events
      val flowEvents = ListBuffer[FlowEvent]()
      for (seq <- seqFlowEvents) {
        flowEvents.append(seq.flowEvent)
      }
      //成功执行的events
      val successEvents = GRPCEventProcessor.this.putEvent(flowEvents.toList)
      //返回
      val headerBuilder = ResponseHeader.newBuilder().setErrno(ERRNO.OK).setTraceInfo(SystemLog.get())
      val replyBuilder = EventBusRPC.EventReplySimple.newBuilder
      if (successEvents.size != flowEvents.size) {
        //必须保证返回和顺序和数量一致
        replyBuilder.setHeader(headerBuilder.setErrno(ERRNO.INVAL).build())
      } else {
        for ((event, i) <- successEvents.zipWithIndex) {
          val seqFlowEvent = seqFlowEvents.get(i)
          if (event != null) {
            replyBuilder.putSeqEventIds(seqFlowEvent.seq, seqFlowEvent.flowEvent.getEventId)
          } else {
            //失败不返回
          }
        }
        replyBuilder.setHeader(headerBuilder.build())
      }
      responseObserver.onNext(replyBuilder.build())
      responseObserver.onCompleted()
      val endTime = System.currentTimeMillis()
      debug(("msg", "remote process events complete"), ("proc_time", s"${endTime - beginTime}ms"))
    }

    override def putEventSync(request: EventBusRPC.SingleEventRequest,
                              responseObserver: StreamObserver[EventBusRPC.SingleEventReply]): Unit = {
      val beginTime = System.currentTimeMillis()
      SystemLog.set(request.getTraceInfo)
      val response = SingleEventReply.newBuilder()
      val responseHeader = ResponseHeader.newBuilder().setTraceInfo(SystemLog.get())
      try {
        val instance = GRPCEventProcessor.this.putEventSync(request.getEvent)
        responseHeader.setErrno(ERRNO.OK)
        response.setInstance(instance)
      } catch {
        case EventProcessException(EventProcessErrorCode.TimeOutError) =>
          responseHeader.setErrno(ERRNO.TIMEOUT)
        case EventProcessException(EventProcessErrorCode.BackPressError) =>
          responseHeader.setErrno(ERRNO.BACKPRESS)
        case EventProcessException(EventProcessErrorCode.CheckError) =>
          responseHeader.setErrno(ERRNO.CHECK)
        case EventProcessException(EventProcessErrorCode.ExecuteError) =>
          responseHeader.setErrno(ERRNO.EXECUTE)
        case EventProcessException(EventProcessErrorCode.CommitError) =>
          responseHeader.setErrno(ERRNO.COMMIT)
        case _ =>
          responseHeader.setErrno(ERRNO.UNKNOWN)
      }
      response.setHeader(responseHeader)
      responseObserver.onNext(response.build())
      responseObserver.onCompleted()
      val endTime = System.currentTimeMillis()
      debug(("msg", "remote process events complete"), ("proc_time", s"${endTime - beginTime}ms"))
    }
  }
}

