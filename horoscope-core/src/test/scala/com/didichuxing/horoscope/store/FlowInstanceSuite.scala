/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.store

import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowEvent.TokenStatus
import com.didichuxing.horoscope.core.FlowRuntimeMessage.{FlowEvent, FlowInstance}
import com.didichuxing.horoscope.util.Logging
import org.scalatest.{BeforeAndAfter, FunSuite}

class FlowInstanceSuite extends FunSuite with BeforeAndAfter with Logging {

  test("flow instance") {
    val event = FlowEvent.newBuilder().setEventId("10000").setTraceId("100000").setFlowName("/root/flow1").build()
    val gotoEvent = FlowEvent.newBuilder().setEventId("10001").setTraceId("100001").setFlowName("/root/flow2")
      .setToken(TokenStatus.newBuilder().setName("flinkId").setValue("f0001").setOwner(""))
      .build()
    val instance = FlowInstance.newBuilder()
      .setFlowId("/root/flow1")
      .setEvent(event)
      .setGoto(gotoEvent)
    print(instance)
    val goto = instance.getGotoBuilder
    val tokenStatus = goto.getTokenBuilder
    tokenStatus.setOwner("trace1")
    goto.setToken(tokenStatus)
    instance.setGoto(goto)
    println(instance)
  }

}
