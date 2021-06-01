/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: liangguorong@didiglobal.com
 */
package com.didichuxing.horoscope.ods

import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowInstance
import com.didichuxing.horoscope.runtime.Value
import com.didichuxing.horoscope.runtime.convert.ValueTypeAdapter
import com.google.gson.{Gson, GsonBuilder, JsonObject}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

final class ProcedureViewBuilder {
  import ProcedureViewBuilder._
  private var fi: FlowInstance = _
  private var proc: FlowInstance.Procedure = _
  private var isDetail: Boolean = true
  private var includeDescendants: Map[String, String] = Map.empty // pid -> parentId
  private var scheduleDescendants: Map[String, Seq[String]] = Map.empty // pid -> scheduled child pid list

  def withFlowInstance(fi: FlowInstance): ProcedureViewBuilder = {
    this.fi = fi
    this
  }

  def withProcedure(p: FlowInstance.Procedure): ProcedureViewBuilder = {
    this.proc = p
    this
  }

  def isDetail(v: Boolean): ProcedureViewBuilder = {
    this.isDetail = v
    this
  }

  def withIncludeDescendants(v: Map[String, String]): ProcedureViewBuilder = {
    this.includeDescendants = v
    this
  }

  def withScheduleDescendants(v: Map[String, Seq[String]]): ProcedureViewBuilder = {
    this.scheduleDescendants = v
    this
  }

  private def buildId(): String = {
    createProcedureId(fi.getEvent.getTraceId, fi.getEvent.getEventId, proc.getScopeList.asScala)
  }

  private def buildArgument(): Map[String, String] = {
    proc.getArgumentMap.asScala.mapValues(Value(_).toJson).toMap
  }

  private def buildResult(): Map[String, String] = {
    val assign = proc.getAssignMap.asScala.mapValues(Value(_).toJson).toMap
    assign
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
      if (isDetail) {
        Fault(f.getCatalog, f.getMessage, f.getDetail)
      } else {
        Fault(f.getCatalog, f.getMessage, "")
      }
    }.toMap
  }

  private def buildLoad(): Array[Load] = {
    proc.getLoadList.asScala.flatMap { l =>
      if (l.hasReference) {
        val ref = l.getReference
        val k = Try(ref.getName.substring(1)).getOrElse(ref.getName)
        val v = Value(l.getValue).toJson
        val pid = createProcedureId(fi.getEvent.getTraceId, ref.getEventId, ref.getScopeList.asScala)
        Some(Load(k, v, pid, ref.getFlowName))
      } else {
        None
      }
    }.toArray
  }

  private def buildAncestor(): String = {
    // check if main procedure
    if (proc.getScopeCount == 0 || (proc.getScopeCount == 1 && proc.getScopeList.contains("main"))) {
      // scheduled event
      if (fi.getEvent.hasParent) {
        val parent = fi.getEvent.getParent
        createProcedureId(parent.getTraceId, parent.getEventId, parent.getScopeList.asScala)
      } else {
        ""
      }
    } else {
      createProcedureId(fi.getEvent.getTraceId,
        fi.getEvent.getEventId, proc.getScopeList.asScala.slice(0, proc.getScopeCount - 1))
    }
  }

  private def buildDescendants(pid: String): Array[String] = {
    val b = new ArrayBuffer[String]()
    this.includeDescendants.foreach { case (id, parentId) =>
      if (parentId == pid) {
        b += id
      }
    }
    if (this.scheduleDescendants.contains(pid)) {
      b ++= this.scheduleDescendants.get(pid).get
    }
    b.sorted.toArray
  }

  private def buildStartTime(): Long = if (proc.getStartTime > 0) proc.getStartTime else fi.getStartTime

  private def buildEndTime(): Long = if (proc.getEndTime > 0) proc.getEndTime else fi.getEndTime

  private def buildExperiment(): String = {
    if (proc.hasExperiment) {
      val e = proc.getExperiment
      val json = new JsonObject()
      json.addProperty("name", e.getName)
      json.addProperty("group", e.getGroup)
      json.toString
    } else {
      "{}"
    }
  }

  private def buildContextChoice(): Array[String] = {
    if (proc.getContextChoiceList != null) {
      proc.getContextChoiceList.asScala.toArray
    } else {
      Array.empty
    }
  }

  private def buildDetail(): ProcedureView = {
    val id = buildId()
    ProcedureView(
      id = id,
      trace_id = fi.getEvent.getTraceId,
      flow_name = proc.getFlowName,
      flow_id = proc.getFlowId,
      start_time = buildStartTime(),
      end_time = buildEndTime(),
      choice = proc.getChoiceList.asScala,
      argument = buildArgument(),
      result = buildResult(),
      composite = buildComposite(),
      fault = buildFault(),
      load = buildLoad(),
      ancestor = buildAncestor(),
      descendants = buildDescendants(id),
      experiment = buildExperiment(),
      context_choice = buildContextChoice()
    )
  }

  private def buildLite(): ProcedureView = {
    val id = buildId()
    ProcedureView(
      id = id,
      trace_id = fi.getEvent.getTraceId,
      flow_name = proc.getFlowName,
      flow_id = proc.getFlowId,
      start_time = buildStartTime(),
      end_time = buildEndTime(),
      choice = proc.getChoiceList.asScala,
      result = buildResult(),
      fault = buildFault(),
      ancestor = buildAncestor(),
      descendants = buildDescendants(id),
      experiment = buildExperiment(),
      context_choice = buildContextChoice()
    )
  }

  def build(): ProcedureView = {
    if (isDetail) {
      buildDetail()
    } else {
      buildLite()
    }
  }

}

object ProcedureViewBuilder {
  implicit val gson: Gson = new GsonBuilder()
    .registerTypeHierarchyAdapter(classOf[Value], new ValueTypeAdapter)
    .serializeNulls()
    .create()

  def buildFrom(fi: FlowInstance, isDetail: Boolean = true): Seq[ProcedureView] = {
    val includeDescendants = fi.getProcedureList.asScala.map { proc =>
      val parentId = createProcedureId(fi.getEvent.getTraceId,
        fi.getEvent.getEventId, proc.getScopeList.asScala.slice(0, proc.getScopeCount - 1))
      val childId = createProcedureId(fi.getEvent.getTraceId, fi.getEvent.getEventId, proc.getScopeList.asScala)
      childId -> parentId
    }.toMap
    val scheduleDescendants: Map[String, Seq[String]] = fi.getScheduleList.asScala.map { sched =>
      val parent = sched.getParent
      val parentId = createProcedureId(parent.getTraceId, parent.getEventId, parent.getScopeList.asScala)
      val scheduleId = createProcedureId(sched.getTraceId, sched.getEventId, Seq("main"))
      parentId -> scheduleId
    }.groupBy(_._1).mapValues(_.distinct.map(_._2).toSeq)
    fi.getProcedureList.asScala.map { proc =>
      val b = new ProcedureViewBuilder()
        .withFlowInstance(fi)
        .withProcedure(proc)
        .isDetail(isDetail)
        .withIncludeDescendants(includeDescendants)
        .withScheduleDescendants(scheduleDescendants)
      b.build()
    }
  }

  private def createProcedureId(traceId: String, eventId: String, scopes: Seq[String]): String = {
    val b = new StringBuilder
    b.append(s"/${traceId}/${eventId}")
    scopes.foreach { c =>
      b.append("/")
      b.append(c)
    }
    b.toString()
  }
}
