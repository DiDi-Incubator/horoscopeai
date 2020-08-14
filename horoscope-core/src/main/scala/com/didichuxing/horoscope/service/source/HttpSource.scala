/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import com.didichuxing.horoscope.core.FlowRuntimeMessage._
import com.didichuxing.horoscope.core.Sources.EventBuilder
import com.didichuxing.horoscope.core._
import com.didichuxing.horoscope.runtime.Implicits.gson
import com.didichuxing.horoscope.runtime.Value
import com.didichuxing.horoscope.util.Utils._
import com.didichuxing.horoscope.util.Logging
import com.typesafe.config.Config

import scala.collection.mutable.ListBuffer

class HttpSourceFactory extends SourceFactory {
  override def newSource(params: Config): Source = {
    new HttpSource(httpEventBuilder())
  }

  def httpEventBuilder(): EventBuilder[Any, FlowEvent] = {
    case (trace: String, flow: String, json: String) =>
      val eventId = getEventId
      val traceId = trace match {
        case "" | "-" | "_" =>
          getTraceId(eventId)
        case _ =>
          trace
      }
      val flowName = s"/$flow"
      val value = gson.fromJson(json, classOf[Value])
      FlowEvent.newBuilder()
        .setEventId(eventId)
        .setFlowName(flowName)
        .setTraceId(traceId)
        .putArgument("@", TraceVariable.newBuilder().setValue(value.as[FlowValue]).build())
        .build()
    case (trace: String, flow: String, params: Map[String, String]) =>
      val eventId = getEventId
      val traceId = trace match {
        case "" | "-" | "_" =>
          getTraceId(eventId)
        case _ =>
          trace
      }
      val flowName = s"/$flow"
      val builder = FlowEvent.newBuilder()
        .setEventId(eventId)
        .setFlowName(flowName)
        .setTraceId(traceId)
      params.foreach {
        case (key, value) =>
          builder.putArgument(s"@$key", TraceVariable.newBuilder().setValue(Value(value).as[FlowValue]).build())
      }
      builder.build()
  }
}

class HttpSource(builder: EventBuilder[Any, FlowEvent]) extends Source with Logging {

  var eventBus: SyncEventBus = _

  override def start(eventBus: EventBus): Unit = {
    eventBus.start()
    this.eventBus = eventBus.asInstanceOf[SyncEventBus]
  }

  override def stop(): Unit = {

  }

  def pushAsync(trace: String, flowName: String, bodys: List[Any]): List[FlowEvent] = {
    val values = ListBuffer[FlowEvent]()
    bodys.foreach(body => {
      values.append(builder((trace, flowName, body)))
    })
    eventBus.doProcess(values.toList)
  }

  def pushSync(trace: String, flowName: String, body: Any): FlowInstance = {
    val startTime = System.currentTimeMillis()
    val instance = eventBus.doProcessSync(builder((trace, flowName, body)))
    info(("msg", "sync process event"), ("proc_time", s"${System.currentTimeMillis() - startTime}ms"))
    instance
  }
}
