/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import com.didichuxing.horoscope.core.FlowRuntimeMessage.{FlowEvent, FlowInstance}
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.util.Logging
import com.typesafe.config.Config

/**
 * 非集群的event bus实现
 *
 * @param sourceName
 * @param flowName
 */
class DefaultEventBus(sourceName: String, flowName: String, params: Config)
                     (implicit ctx: ApplicationContext)
  extends AbstractEventBus(sourceName, flowName, params) with Logging {

  override def newEventProcessor(): EventProcessor = {
    new DefaultEventProcessor(sourceName, params)
  }

  override def doProcess(events: List[FlowEvent]): List[FlowEvent] = {
    eventProcessor.putEvent(events)
  }

  override def doProcessSync(event: FlowEvent): FlowInstance = {
    eventProcessor.putEventSync(event)
  }
}
