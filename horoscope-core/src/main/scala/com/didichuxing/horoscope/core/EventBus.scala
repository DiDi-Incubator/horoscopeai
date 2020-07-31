/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.core

import com.didichuxing.horoscope.core.FlowRuntimeMessage.{FlowEvent, FlowInstance}
import com.didichuxing.horoscope.runtime.Value

trait EventBus {

  def start()

  def stop()

  def process(events: List[Value]): List[FlowEvent]

  def doProcess(events: List[FlowEvent]): List[FlowEvent]
}

trait SyncEventBus extends EventBus {
  def processSync(event: Value): FlowInstance
}
