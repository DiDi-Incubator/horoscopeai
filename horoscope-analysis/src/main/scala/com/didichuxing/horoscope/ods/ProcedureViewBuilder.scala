/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: liangguorong@didiglobal.com
 */
package com.didichuxing.horoscope.ods

import com.didichuxing.horoscope.core.FlowRuntimeMessage.{FlowEvent, FlowInstance}
import com.didichuxing.horoscope.runtime.Value
import com.didichuxing.horoscope.runtime.convert.ValueTypeAdapter
import com.google.gson.{Gson, GsonBuilder}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

final class ProcedureViewBuilder {
  import ProcedureViewBuilder._
  private var fi: FlowInstance = _
  private var proc: FlowInstance.Procedure = _

  def withFlowInstance(fi: FlowInstance): ProcedureViewBuilder = {
    this.fi = fi
    this
  }

  def withProcedure(p: FlowInstance.Procedure): ProcedureViewBuilder = {
    this.proc = p
    this
  }

  private def buildId(): String = {
    val b = new ArrayBuffer[String]()
    b ++= Array("", fi.getEvent.getTraceId, fi.getEvent.getEventId)
    b ++= proc.getScopeList.asScala
    b.mkString("/")
  }

  private def buildArgument(): Map[String, String] = {
    proc.getArgumentMap.asScala.mapValues(Value(_).toJson).toMap
  }

  private def buildResult(): Map[String, String] = {
    val assign = proc.getAssignMap.asScala.mapValues(Value(_).toJson).toMap
    val composite = proc.getCompositeMap.asScala.mapValues(c => Value(c.getResult).toJson).toMap
    (assign ++ composite)
  }

  private def buildComposite(): Map[String, Composite] = {
    proc.getCompositeMap.asScala.mapValues { c =>
      Composite(
        c.getCompositor,
        Value(c.getArgument).toJson,
        Value(c.getResult).toJson,
        c.getStartTime,
        c.getEndTime,
        c.getBatchSize
      )
    }.toMap
  }

  private def buildFault(): Map[String, Fault] = {
    proc.getFaultMap.asScala.mapValues { f =>
      Fault(f.getCatalog, f.getMessage, f.getDetail)
    }.toMap
  }

  def build(): ProcedureView = {
    ProcedureView(
      fi.getEvent.getTraceId,
      buildId(),
      fi.getEvent.getFlowName,
      if (proc.getStartTime > 0) proc.getStartTime else fi.getStartTime,
      if (proc.getEndTime > 0) proc.getEndTime else fi.getEndTime,
      proc.getChoiceList.asScala,
      buildArgument(),
      buildResult(),
      buildComposite(),
      buildFault()
    )
  }

  def buildLite(): ProcedureView = {
    ProcedureView(
      fi.getEvent.getTraceId,
      buildId(),
      fi.getEvent.getFlowName,
      if (proc.getStartTime > 0) proc.getStartTime else fi.getStartTime,
      if (proc.getEndTime > 0) proc.getEndTime else fi.getEndTime,
      choice = Seq.empty[String],
      argument = Map.empty[String, String],
      buildResult(),
      composite = Map.empty[String, Composite],
      fault = Map.empty[String, Fault]
    )
  }

}

object ProcedureViewBuilder {
  implicit val gson: Gson = new GsonBuilder()
    .registerTypeHierarchyAdapter(classOf[Value], new ValueTypeAdapter)
    .serializeNulls()
    .create()
}
