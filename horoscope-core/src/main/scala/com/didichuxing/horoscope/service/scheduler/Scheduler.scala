/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.scheduler

import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowEvent
import com.didichuxing.horoscope.core.SourceFactory
import com.didichuxing.horoscope.runtime.FlowExecutor
import com.didichuxing.horoscope.service.resource.ResourceManager

trait Scheduler {
  def start(flowExecutor: FlowExecutor, sourceFactories: Map[String, SourceFactory])

  def stop()

  def onTimeSpanSwitch(timestamp: Long)

  def goto(event: FlowEvent)

  def recovery(resourceManager: ResourceManager)

}
