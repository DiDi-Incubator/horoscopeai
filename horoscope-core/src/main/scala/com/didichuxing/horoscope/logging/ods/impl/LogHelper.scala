/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: liangguorong@didiglobal.com
 */

package com.didichuxing.horoscope.logging.ods.impl

import java.util.Base64

import com.didichuxing.horoscope.core.FlowRuntimeMessage
import com.didichuxing.horoscope.core.FlowRuntimeMessage.{FlowInstance, FlowInstanceOrBuilder, FlowValue}
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

  // deprecated in v2
  def simplifyLog(flowInstance: FlowInstance): FlowRuntimeMessage.FlowInstance = {
    val b = flowInstance.toBuilder
    b.clearAssign()
    flowInstance.getAssignList.foreach { a =>
      if (a.getName.startsWith("-") || a.getName().startsWith("~")) {
        // ignore value
        val ta = b.addAssignBuilder()
          .setValue(FlowValue.newBuilder())
          .setName(a.getName)
        if (a.hasChoice) ta.setChoice(a.getChoice)
        if (a.hasCompositor) ta.setCompositor(a.getCompositor)
        if (a.getDependencyList.size() > 0) ta.addAllDependency(a.getDependencyList)
        if (a.hasError) ta.setError(a.getError)
        if (a.getCompositeArgumentCount > 0) ta.putAllCompositeArgument(a.getCompositeArgumentMap)
        b.addAssign(ta)
      } else {
        b.addAssign(a)
      }
    }
    b.build()
  }

  // deprecated in v2
  def ignoreLog(flowInstance: FlowInstance): Boolean = {
    val logIgnoreOption = flowInstance.getAssignList.find(_.getName == "flow_log_ignore")
    if (logIgnoreOption.isDefined) {
      Try(Value(logIgnoreOption.get.getValue).as[Boolean]).getOrElse(false)
    } else {
      false
    }
  }

}
