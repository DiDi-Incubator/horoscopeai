/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: liangguorong@didiglobal.com
 */
package com.didichuxing.horoscope.ods

import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowInstance
import com.didichuxing.horoscope.runtime.{NULL, Value, ValueDict}
import com.didichuxing.horoscope.runtime.convert.ValueTypeAdapter
import com.google.gson.{Gson, GsonBuilder}

object Implicits {
  implicit val gson: Gson = new GsonBuilder()
    .registerTypeHierarchyAdapter(classOf[Value], new ValueTypeAdapter)
    .serializeNulls()
    .create()

  implicit class ProcedureViewHelper(view: ProcedureView) {
    def toJson(): String = Value(view).toJson
  }

  implicit class FlowInstanceHelper(instance: FlowInstance) {
    import com.didichuxing.horoscope.runtime.Implicits.builtin
    private val v = Value(instance).asInstanceOf[ValueDict]
    val traceId = instance.getEvent.getTraceId
    val eventId = instance.getEvent.getEventId
    val flowName = instance.getEvent.getFlowName
    val flowStartTime = instance.getStartTime
    val flowEndTime = instance.getEndTime

    def eval(code: String): Value = v.eval(code)

    def get(name: String): Value = {
      try {
        val a = eval(s"procedure[0].assign.${name}")
        if (a != NULL) {
          a
        } else {
          eval(s"procedure[0].composite.${name}.result")
        }
      } catch {
        case e: Exception =>
          NULL
      }
    }

    def toJson(implicit gson: Gson): String = v.toJson
  }


}
