/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.mock

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowEvent.TokenStatus
import com.didichuxing.horoscope.core.FlowRuntimeMessage.{FlowEvent, FlowInstance}
import com.didichuxing.horoscope.runtime.FlowExecutor
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.util.{Logging, PublicLog, SystemLog}
import com.google.protobuf.util.JsonFormat

import scala.concurrent.Future
import scala.collection.JavaConversions._
import scala.concurrent.duration._

case class TraceFlowEvent(logId: String, flowEvent: FlowEvent)

class MockFlowExecutor(implicit ctx: ApplicationContext) extends FlowExecutor with Logging {

  implicit val flowStore = ctx.flowStore
  implicit val traceStore = ctx.traceStore
  implicit val compositorFactories = ctx.compositorFactories
  implicit val system = ctx.system
  var flowExecutorActorRef: ActorRef = _
  val pubLog = PublicLog(ctx.config)

  override def start(): Unit = {
    flowExecutorActorRef = system.actorOf(Props(new FlowExecutorActor), "FlowExecutorActor")
  }

  override def stop(): Unit = {

  }

  override def execute(event: FlowEvent): Future[FlowInstance] = {
    //当前线程的trace信息，需要带到actor的消息中
    val f = flowExecutorActorRef.ask(TraceFlowEvent(SystemLog.get(), event))(Timeout(3 seconds))
    f.asInstanceOf[Future[FlowInstance]]
  }

  class FlowExecutorActor extends Actor {
    val jsonPrinter = JsonFormat.printer.omittingInsignificantWhitespace()

    override def receive: Receive = {
      case traceEvent: TraceFlowEvent =>
        //将前一个线程的trace信息带人当前actor的dispatcher的线程
        SystemLog.set(traceEvent.logId)
        val event = traceEvent.flowEvent
        info(("msg", "flow executor event"), ("eventId", event.getEventId), ("traceId", event.getTraceId))
        Thread.sleep(10)
        val flowName = event.getFlowName
        val f = flowStore.getFlowByName(flowName)
        if (f.isEmpty) {
          throw new Exception(s"flow ${flowName} not found")
        }
        val flow = f.get
        val statements = flow.getBody.getStatementList
        for (statement <- statements) {
          if (statement.hasCompositeStatement) {
            //traceStore.getContext()
            //todo: execute statement
          }
        }
        val flowInstance = FlowInstance.newBuilder().setEvent(event).setFlowId(flow.getId)
        if (flowName == "/root/flow1") {
          val gotoEvent = FlowEvent.newBuilder()
            .setEventId("")
            .setTraceId(event.getTraceId)
            .setFlowName("/root/flow-goto1")
            .setScheduledTimestamp(System.currentTimeMillis() + 1000 * 60)
            .setToken(TokenStatus.newBuilder().setName("token1").setValue("link123").setOwner(""))
          flowInstance.setGoto(gotoEvent)
        }
        pubLog.public(("flowEvent", jsonPrinter.print(event)), ("flowInstance", jsonPrinter.print(flowInstance)))
        sender() ! flowInstance.build()
      case msg@_ =>
        error(("msg", s"unknow msg:${msg}"))
    }
  }

}