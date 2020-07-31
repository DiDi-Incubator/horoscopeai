/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: chaiyi@didiglobal.com
 */

package com.didichuxing.horoscope.util

import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowValue
import com.google.protobuf.DynamicMessage
import org.scalatest.FunSuite

class PBParserUtilSuite extends FunSuite with Logging {
  test("pb message parser") {
    val dict = FlowValue.Dict.newBuilder()
      .putChild("k1", FlowValue.newBuilder().setIntegral(1000).build())
      .putChild("k2", FlowValue.newBuilder().setText("abc").build())
      .putChild("k3", FlowValue.newBuilder().setBoolean(true).build())
      .putChild("k4", FlowValue.newBuilder().setList(FlowValue.List.newBuilder()
        .addChild(FlowValue.newBuilder().setIntegral(1))).build())
    val flowValue = FlowValue.newBuilder().setDict(dict).build()
    val bytes = flowValue.toByteArray
    val path = "protobuf/descriptor-sets/horoscope-0.1.0-SNAPSHOT.protobin"
    val messageMap = PBParserUtil.loadDescriptors(Array(path))
    val className = "com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowValue"
    val parsed = DynamicMessage.parseFrom(messageMap(className), bytes)
    info(parsed.toString)
  }
}
