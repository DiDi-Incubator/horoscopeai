/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import com.didichuxing.horoscope.core.FlowRuntimeMessage.{FlowEvent, FlowInstance}

/**
 * event bus的内部引用，用来消息的反压，和发送到flowExecutor执行
 */
trait EventProcessor {

  def start()

  def stop()

  def putEvent(events: List[FlowEvent]): List[FlowEvent]

  def putEventSync(event: FlowEvent): FlowInstance
}
