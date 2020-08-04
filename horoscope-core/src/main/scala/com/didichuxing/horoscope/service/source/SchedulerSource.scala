/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowEvent
import com.didichuxing.horoscope.core.Sources.EventBuilder
import com.didichuxing.horoscope.core.{EventBus, FlowRuntimeMessage, Source}
import com.didichuxing.horoscope.runtime.Value
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.util.Logging
import com.typesafe.config.Config

/**
 * http://wiki.intra.xiaojukeji.com/pages/viewpage.action?pageId=322779383
 */
class SchedulerSource(sourceConfig: Config, builder: EventBuilder[List[Array[Byte]], List[Value]])
  extends Source with Logging {

  var eventBus: EventBus = _

  override def start(eventBus: EventBus): Unit = {
    eventBus.start()
    this.eventBus = eventBus
  }

  override def stop(): Unit = {
    eventBus.stop()
  }

  def push(values: List[Array[Byte]]): Unit = {
    val events = builder(values)
    try {
      eventBus.process(events)
    } catch {
      case ex: Exception =>
        //可能出现格式错误等异常，这里忽略消息
        error(("msg", "scheduler push error"), ("ex", ex.getMessage), ("events", events))
    }
  }
}

class SchedulerEventBus(sourceName: String, params: Config)
                       (implicit ctx: ApplicationContext) extends EventBus with Logging {

  val eventProcessor = new SchedulerEventProcessor(sourceName, params)

  override def start(): Unit = {
    eventProcessor.start()
  }

  override def stop(): Unit = {
    eventProcessor.stop()
  }

  override def process(events: List[Value]): List[FlowRuntimeMessage.FlowEvent] = {
    val flowEvents = EventBuilders.schedulerFlowEventBuilder(events)
    //process events
    eventProcessor.putEvent(flowEvents)
  }

  override def doProcess(events: List[FlowEvent]): List[FlowEvent] = {
    eventProcessor.putEvent(events)
  }

  override def process(flowName: String, events: List[Value]): List[FlowEvent] = {
    process(events)
  }
}

class SchedulerEventProcessor(sourceName: String, params: Config)(implicit ctx: ApplicationContext)
  extends DefaultEventProcessor(sourceName, params) with Logging {

  override def putEvent(events: List[FlowEvent]): List[FlowEvent] = {
    info(("msg", "available backpress"), ("source", sourceName), ("count", backPress.availablePermits()))
    //scheduler source 不需要反压超时，也不需要写入mailbox
    backPress.acquire(events.size)
    for (flowEvent <- events) {
      debug(("msg", "putEvent"), ("event trace", flowEvent.getTraceId))
      exec(flowEvent)
    }
    events
  }
}
