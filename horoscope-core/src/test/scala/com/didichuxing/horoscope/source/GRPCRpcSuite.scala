/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.source

import java.util.concurrent.Executor

import com.didichuxing.horoscope.core.EventBusRPC.ResponseHeader.ERRNO
import com.didichuxing.horoscope.core.EventBusRPC.{ResponseHeader}
import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowEvent
import com.didichuxing.horoscope.core._
import com.didichuxing.horoscope.service.resource.Participant
import com.didichuxing.horoscope.service.source._
import com.didichuxing.horoscope.util.{Logging, SystemLog}
import com.typesafe.config.ConfigFactory
import io.grpc.Server
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import io.grpc.stub.StreamObserver

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object GRPCRpcSuite extends App {
  var server: Server = _

  startRPCServer()

  def startRPCServer(): Unit = {
    val port = 6880
    //grpc flow control window, default 100MB
    val window = 102400 * 1024
    server = NettyServerBuilder.forPort(port).flowControlWindow(window)
      .addService(new GRPCEventBusServer).build.start
  }

  val client = new GrpcEventBusClient
  val flowEvents = ListBuffer[SeqFlowEvent]()
  val event = FlowEvent.newBuilder().setEventId("event1").setTraceId("trace1").setFlowName("/root/flow1").build()
  for (i <- 0 until 100) {
    flowEvents.append(SeqFlowEvent(i, event))
  }
  val startTime = System.currentTimeMillis()
  for (_ <- 0 until 10000) {
    client.process(flowEvents.toList)
  }
  println(System.currentTimeMillis() - startTime)
  client.stop()
  server.shutdown()
}

class GrpcEventBusClient extends Logging {

  val participant = new Participant("localhost", 2552)
  val map = Map[String, Any]("rpc.port" -> 6880)
  val params = ConfigFactory.parseMap(map)
  implicit val ec: Executor = null
  val gepc = new GRPCEventProcessorClient(participant, params)

  def process(flowEvents: List[SeqFlowEvent]): List[SeqFlowEvent] = {
    val seqEventMap = mutable.Map[String, FlowEvent]()
    val request = EventBusRPC.EventRequestSimple.newBuilder().setTraceInfo(SystemLog.get())
    flowEvents.foreach(seqFlowEvent => {
      val flowEvent = seqFlowEvent.flowEvent
      request.putSeqEvents(seqFlowEvent.seq, flowEvent)
      seqEventMap.put(flowEvent.getEventId, flowEvent)
    })
    val stub = gepc.stub
    val lf = stub.putEventSimple(request.build())
    val result = lf.get()
    val seqList = ListBuffer[SeqFlowEvent]()
    val errno = result.getHeader.getErrno
    if (errno == ERRNO.OK) {
      val seqEventIds = result.getSeqEventIdsMap
      if (seqEventIds != null) {
        seqEventIds.foreach {
          case (seq, eventId) =>
            if (eventId != null && seqEventMap.get(eventId).isDefined) {
              seqList.add(SeqFlowEvent(seq, seqEventMap.get(eventId).get))
            }
        }
      }
      seqList.toList
    } else {
      error(("msg", "event bus rpc callback"), ("code", errno))
      throw new Exception("event bus rpc callback")
    }
  }

  def stop(): Unit = {
    gepc.stop()
  }
}

class GRPCEventBusServer extends EventProcessorServiceGrpc.EventProcessorServiceImplBase {

  override def putEvent(request: EventBusRPC.EventRequest,
                        responseObserver: StreamObserver[EventBusRPC.EventReply]): Unit = {

  }

  override def putEventSimple(request: EventBusRPC.EventRequestSimple,
                              responseObserver: StreamObserver[EventBusRPC.EventReplySimple]): Unit = {

    SystemLog.set(request.getTraceInfo)
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
    val successEvents = flowEvents.toList
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
  }

  override def putEventSync(request: EventBusRPC.SingleEventRequest,
                            responseObserver: StreamObserver[EventBusRPC.SingleEventReply]): Unit = {
  }
}