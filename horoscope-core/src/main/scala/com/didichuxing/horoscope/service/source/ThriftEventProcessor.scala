/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowEvent
import com.didichuxing.horoscope.core.event_busConstants._
import com.didichuxing.horoscope.core._
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.util.{Logging, SystemLog}
import com.typesafe.config.Config
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.server.{TServer, TThreadPoolServer, TThreadedSelectorServer}
import org.apache.thrift.transport.{TNonblockingServerSocket, TServerSocket}

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import scala.util.Try

class ThriftEventProcessor(sourceName: String, params: Config)(implicit ctx: ApplicationContext)
  extends RPCEventProcessor(sourceName, params) with Logging {

  val port = params.getInt("rpc.port")
  val clientSize = Try(params.getInt("rpc.client-size")).getOrElse(1)
  val nioEnable = Try(params.getBoolean("rpc.nio")).getOrElse(false)
  var server: TServer = _

  override def startRPCServer(): Unit = {
    if(nioEnable) {
      startNIO()
    } else {
      startBIO()
    }
  }

  private def startBIO(): Unit = {
    val serverTransport = new TServerSocket(port)
    val serverArgs = new TThreadPoolServer.Args(serverTransport)
      .requestTimeout(900) //15分钟
    val processor = new EventBusService.Processor(new EventProcessorServiceImpl)
    val protocolFactory = new TBinaryProtocol.Factory(true, true)
    serverArgs.processor(processor)
    serverArgs.protocolFactory(protocolFactory)
    server = new TThreadPoolServer(serverArgs)
    ec.execute(new Runnable {
      override def run(): Unit = {
        server.serve()
      }
    })
  }

  //https://www.cnblogs.com/exceptioneye/p/4945073.html
  private def startNIO(): Unit = {
    //client must TFramedTransport
    val tServerSocket = new TNonblockingServerSocket(port)
    val processor = new EventBusService.Processor(new EventProcessorServiceImpl)
    val protoFactory = new TBinaryProtocol.Factory(true, true) //TTransportFactory
    val serverArgs = new TThreadedSelectorServer.Args(tServerSocket)
      .processor(processor)
      .protocolFactory(protoFactory)
      .selectorThreads(clientSize)
    server = new TThreadedSelectorServer(serverArgs)
    ec.execute(new Runnable {
      override def run(): Unit = {
        server.serve()
      }
    })
  }

  override def stopRPCServer(): Unit = {
    if (server != null) {
      server.stop()
    }
  }

  class EventProcessorServiceImpl extends EventBusService.Iface {

    override def putEvent(request: EventRequest): EventReply = {
      val beginTime = System.currentTimeMillis()
      SystemLog.set(request.getTraceInfo)
      debug(("msg", "remote receive events"), ("server", resourceManager.local().getParticipantId()))
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
      val successEvents = ThriftEventProcessor.this.putEvent(flowEvents.toList)
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
      val endTime = System.currentTimeMillis()
      info(("msg", "remote process events complete"), ("proc_time", s"${endTime - beginTime}ms"))
      reply
    }

    override def putEventSync(request: SingleEventRequest): SingleEventReply = {
      val beginTime = System.currentTimeMillis()
      SystemLog.set(request.getTraceInfo)
      val response = new SingleEventReply()
      val responseHeader = new ResponseHeader().setTraceInfo(SystemLog.get())
      try {
        val eventRaw = request.getEvent
        val instance = ThriftEventProcessor.this.putEventSync(FlowEvent.parseFrom(eventRaw))
        responseHeader.setErrno(ERRNO_OK)
        response.setInstance(instance.toByteArray)
      } catch {
        case EventProcessException(EventProcessErrorCode.TimeOutError) =>
          responseHeader.setErrno(ERRNO_TIMEOUT)
        case EventProcessException(EventProcessErrorCode.BackPressError) =>
          responseHeader.setErrno(ERRNO_BACKPRESS)
        case EventProcessException(EventProcessErrorCode.CheckError) =>
          responseHeader.setErrno(ERRNO_CHECK)
        case EventProcessException(EventProcessErrorCode.ExecuteError) =>
          responseHeader.setErrno(ERRNO_EXECUTE)
        case EventProcessException(EventProcessErrorCode.CommitError) =>
          responseHeader.setErrno(ERRNO_COMMIT)
        case _ =>
          responseHeader.setErrno(ERRNO_UNKNOWN)
      }
      response.setHeader(responseHeader)
      val endTime = System.currentTimeMillis()
      debug(("msg", "remote process events complete"), ("proc_time", s"${endTime - beginTime}ms"))
      response
    }
  }

}
