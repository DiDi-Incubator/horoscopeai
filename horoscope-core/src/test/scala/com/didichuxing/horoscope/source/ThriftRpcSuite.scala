/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.source

import java.nio.ByteBuffer

import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowEvent
import com.didichuxing.horoscope.core.event_busConstants.{ERRNO_INVAL, ERRNO_OK}
import com.didichuxing.horoscope.core._
import com.didichuxing.horoscope.service.resource.Participant
import com.didichuxing.horoscope.service.source.{SeqFlowEvent, ThriftEventProcessorClient}
import com.didichuxing.horoscope.source.GRPCRpcSuite.{event, flowEvents}
import com.didichuxing.horoscope.util.{Logging, SystemLog}
import com.typesafe.config.ConfigFactory
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.server.{TServer, TThreadedSelectorServer}
import org.apache.thrift.transport.{TNonblockingServerSocket, TTransportException}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object ThriftRpcSuite extends App {
  var server: TServer = _

  startNIO()
  private def startNIO(): Unit = {
    //client must TFramedTransport
    val tServerSocket = new TNonblockingServerSocket(6880)
    val processor = new EventBusService.Processor(new ThriftEventBusServer)
    val protoFactory = new TBinaryProtocol.Factory(true, true) //TTransportFactory
    val serverArgs = new TThreadedSelectorServer.Args(tServerSocket)
      .processor(processor)
      .protocolFactory(protoFactory)
      .selectorThreads(5)
    server = new TThreadedSelectorServer(serverArgs)
    new Thread(new Runnable {
      override def run(): Unit = {
        server.serve()
      }
    }).start()
  }

  val client = new ThriftEventBusClient
  val flowEvents = ListBuffer[SeqFlowEvent]()
  val event = FlowEvent.newBuilder().setEventId("event1").setTraceId("trace1").setFlowName("/root/flow1").build()
  for (i <- 0 until 100) {
    flowEvents.append(SeqFlowEvent(i, event))
  }
  val startTime = System.currentTimeMillis()
  for(_ <- 0 until 10000) {
    client.process(flowEvents.toList)
  }
  println(System.currentTimeMillis() - startTime)

  client.stop()
  server.stop()
}

class ThriftEventBusClient extends Logging {

  val participant = new Participant("localhost", 2552)
  val map = Map[String, Any]("rpc.port" -> 6880, "rpc.client-size" -> 5, "rpc.nio" -> true)
  val params = ConfigFactory.parseMap(map)
  val tepc = new ThriftEventProcessorClient(participant, params)

  def process(flowEvents: List[SeqFlowEvent]): List[SeqFlowEvent] = {
    val eventMap = mutable.Map[String, FlowEvent]()
    val request = new EventRequest()
    request.setTraceInfo(SystemLog.get())
    flowEvents.foreach(v => {
      eventMap.put(v.flowEvent.getEventId, v.flowEvent)
      request.putToSeqEvents(v.seq, ByteBuffer.wrap(v.flowEvent.toByteArray))
    })

    val stub = tepc.stub
    try {
      val result = stub.putEvent(request)
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
        seqList.toList
      } else {
        error(("msg", "event bus rpc callback"), ("code", errno))
        throw new Exception("event bus rpc callback")
      }
    } catch {
      case ex: TTransportException =>
        error(("msg", "thrift transport error"), ("type", ex.getType), ("ex", ex.getCause))
        //异常关闭连接，重连
        tepc.reconnect(stub)
        throw new Exception("thrift transport error")
      case ex: Exception =>
        ex.printStackTrace()
        throw ex
    } finally {
      tepc.release(stub)
    }
  }

  def stop(): Unit = {
    tepc.stop()
  }
}

class ThriftEventBusServer extends EventBusService.Iface {
  override def putEvent(request: EventRequest): EventReply = {
    SystemLog.set(request.getTraceInfo)
    //带序号的events
    val seqEventsMap = request.getSeqEvents
    val seqFlowEvents = ListBuffer[SeqFlowEvent]()
    for ((seq, eventRaw) <- seqEventsMap) {
      seqFlowEvents.append(SeqFlowEvent(seq, FlowEvent.parseFrom(eventRaw)))
    }
    //不带序号的events
    val flowEvents = ListBuffer[FlowEvent]()
    for (seq <- seqFlowEvents) {
      flowEvents.append(seq.flowEvent)
    }
    //成功执行的events
    val successEvents = flowEvents.toList
    //返回
    val reply = new EventReply
    val header = new ResponseHeader().setTraceInfo(SystemLog.get())
    if (successEvents.size != flowEvents.size) {
      //必须保证返回和顺序和数量一致
      header.setErrno(ERRNO_INVAL)
    } else {
      //返回成功执行的带序号的events
      header.setErrno(ERRNO_OK)
      for ((event, i) <- successEvents.zipWithIndex) {
        val seqFlowEvent = seqFlowEvents.get(i)
        if (event != null) {
          reply.putToSeqEventIds(seqFlowEvent.seq, seqFlowEvent.flowEvent.getEventId)
        } else {
          //失败不返回
        }
      }
    }
    reply.setHeader(header)
    reply
  }

  override def putEventSync(request: SingleEventRequest): SingleEventReply = {
    val response = new SingleEventReply()
    response
  }
}