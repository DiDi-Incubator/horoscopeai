/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: liangguorong@didiglobal.com
 */

package com.didichuxing.horoscope.logging.ods.impl

import java.util.Base64

import com.didichuxing.horoscope.core.FlowRuntimeMessage
import com.didichuxing.horoscope.core.FlowRuntimeMessage.{FlowInstance, FlowValue}
import com.didichuxing.horoscope.runtime.Value
import com.didichuxing.horoscope.runtime.convert.ValueTypeAdapter
import com.google.gson.{Gson, GsonBuilder}

import scala.util.Try

trait LogHelper {
  import scala.collection.JavaConversions._
  implicit val gson: Gson = {
    new GsonBuilder()
      .registerTypeHierarchyAdapter(classOf[Value], new ValueTypeAdapter)
      .create()
  }
  implicit class FlowInstanceConverter(msg: FlowInstance) {
    def toJson(): String = Value(msg).toJson
    def toBase64: String = Base64.getEncoder.encodeToString(msg.toByteArray)
  }
}
