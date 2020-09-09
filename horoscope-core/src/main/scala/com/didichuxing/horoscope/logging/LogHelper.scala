package com.didichuxing.horoscope.logging

import java.util.Base64

import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowInstance
import com.didichuxing.horoscope.runtime.Value
import com.didichuxing.horoscope.runtime.convert.ValueTypeAdapter
import com.google.gson.{Gson, GsonBuilder}

trait LogHelper {
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
