/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.store

import com.didichuxing.horoscope.core.FlowRuntimeMessage.{FlowEvent, FlowInstance, FlowValue}
import com.didichuxing.horoscope.service.storage.HBaseTraceStore
import com.didichuxing.horoscope.util.Utils._
import com.didichuxing.horoscope.util.Logging
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfter, FunSuite, Ignore}

@Ignore
class TraceStoreSuite extends FunSuite with BeforeAndAfter with Logging {

  var traceStore: HBaseTraceStore = _
  var hconf: Config = _

  before {
    hconf = ConfigFactory.load("application-remoting-2552.conf")
    traceStore = new HBaseTraceStore()
    traceStore.start(hconf)
  }

  test("trace context") {
    val flowName = "/root/flow1"
    val gotoFlowName = "/root/flow-goto-demo"
    val source = "source1"
    val traceId = "trace1"
    val event = FlowEvent.newBuilder()
      .setEventId("100000")
      .setTraceId(traceId)
      .setFlowName(flowName)
    //1. add event 10000
    val flowEvent = traceStore.addEvent(source, event)
    info(("add event", flowEvent.getEventId))
    //info(("source1 event list", traceStore.getEventsBySource(source, 0, 999999)))
    val gotoEvent = FlowEvent.newBuilder()
      .setEventId("100001")
      .setTraceId(traceId)
      .setFlowName(gotoFlowName)
      .setScheduledTimestamp(System.currentTimeMillis())
      .build()
    val instance = FlowInstance.newBuilder()
      .setFlowId("")
      .setEvent(event)
    //2. commit event10000
    val flowInstance = traceStore.commitEvent(source, instance)
    info(("commit event 1", flowInstance.getEvent.getEventId))
    //info(("scheduler event list 1", traceStore.getEventsBySource(SCH_STORE_COL, 0, 999999)))
    //3. commit event10001
    val instance2 = FlowInstance.newBuilder()
      .setFlowId("")
    val flowInstance2 = traceStore.commitEvent(schStoreCol(hconf), instance2)
    info(("commit event 2", flowInstance2.getEvent.getEventId))
    //info(("scheduler event list 2", traceStore.getEventsBySource(SCH_STORE_COL, 0, 999999)))
    val instance3 = FlowInstance.newBuilder()
      .setFlowId("")
    val flowInstance3 = traceStore.commitEvent(schStoreCol(hconf), instance3)
    info(("commit event 3", flowInstance3.getEvent.getEventId))
  }

  test("source recovery") {
    val events = traceStore.getEventsBySource("s1", 0, 99999)
    events.foreach(e => println(e.getEventId))
  }

  test("pool scheduler") {
    val events = traceStore.pollSchedulerEvents("s1", 0, System.currentTimeMillis(), 6)
    events.foreach(e => println(e.getEventId))
  }
}
