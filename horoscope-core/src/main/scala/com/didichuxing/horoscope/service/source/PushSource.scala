/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import com.didichuxing.horoscope.core.FlowRuntimeMessage._
import com.didichuxing.horoscope.core.Sources.EventBuilder
import com.didichuxing.horoscope.core._
import com.didichuxing.horoscope.runtime.Value
import com.didichuxing.horoscope.util.Logging
import com.typesafe.config.Config

import scala.collection.mutable.ListBuffer

class PushSourceFactory[T](builder: EventBuilder[T, Value]) extends SourceFactory {
  override def newSource(params: Config): Source = {
    new PushSource(builder)
  }
}

class PushSource[T](builder: EventBuilder[T, Value]) extends Source with Logging {

  var eventBus: SyncEventBus = _

  override def start(eventBus: EventBus): Unit = {
    eventBus.start()
    this.eventBus = eventBus.asInstanceOf[SyncEventBus]
  }

  override def stop(): Unit = {

  }

  def pushAsync(flowName: String, raws: List[T]): List[FlowEvent] = {
    val values = ListBuffer[Value]()
    raws.foreach(v => {
      values.append(builder(v))
    })
    eventBus.process(flowName, values.toList)
  }

  def pushSync(flowName: String, raw: T): FlowInstance = {
    val startTime = System.currentTimeMillis()
    val instance = eventBus.processSync(flowName, builder(raw))
    info(("msg", "sync process event"), ("proc_time", s"${System.currentTimeMillis() - startTime}ms"))
    instance
  }
}
