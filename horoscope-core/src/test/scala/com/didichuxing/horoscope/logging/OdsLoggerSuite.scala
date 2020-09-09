/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: liangguorong@didiglobal.com
 */

package com.didichuxing.horoscope.logging

import java.util.UUID

import com.didichuxing.horoscope.core.FlowRuntimeMessage.{FlowEvent, FlowInstance}
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

class OdsLoggerSuite extends FunSuite {

  test("public log") {
    val config = ConfigFactory.empty()
    val logger = OdsLoggerFactory.newLocalLogger(config)
    val flowInstance = FlowInstance.newBuilder()
      .setFlowId(UUID.randomUUID().toString)
      .setEvent(FlowEvent.newBuilder().setEventId(UUID.randomUUID().toString)
        .setTraceId(UUID.randomUUID().toString).setFlowName("/test"))
      .build()
    logger.log(flowInstance)
  }

}
