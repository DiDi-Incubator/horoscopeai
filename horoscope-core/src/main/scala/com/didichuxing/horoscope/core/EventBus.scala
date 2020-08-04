/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.core

import com.didichuxing.horoscope.core.FlowRuntimeMessage.{FlowEvent, FlowInstance}
import com.didichuxing.horoscope.runtime.Value

trait EventBus {
  //启动
  def start()
  //停止
  def stop()
  //异步执行默认flow，Value->FlowEvent
  def process(events: List[Value]): List[FlowEvent]
  //异步执行指定flow，Value->FlowEvent
  def process(flowName: String, events: List[Value]): List[FlowEvent]
  //不转换类型，直接执行
  def doProcess(events: List[FlowEvent]): List[FlowEvent]
}

trait SyncEventBus extends EventBus {
  //同步执行默认flow，Value->FlowEvent
  def processSync(event: Value): FlowInstance
  //同步执行指定flow，Value->FlowEvent
  def processSync(flowName: String, event: Value): FlowInstance
  //不转换类型，直接执行
  def doProcessSync(event: FlowEvent): FlowInstance
}
