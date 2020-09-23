/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: liangguorong@didiglobal.com
 */

package com.didichuxing.horoscope.ods

// name style is for warehouse table convention
case class Composite(
  compositor: String,
  argument: String,
  result: String,
  start_time: Long,
  end_time: Long,
  batch_size: Long
)

case class Fault(
  catalog: String,
  message: String,
  detail: String
)

case class Load(
  key: String,
  value: String,
  pid: String,
  flow_name: String
)

/**
 * ProcedureView is based on 4w1h principle
 * @param id procedure id, /trace_id/event_id/scope1/scope2/...
 * @param trace_id Who, each trace contains multiple procedure events
 * @param flow_name Where, flow_name such as /traffic/intelligence/procedure/info-audit
 * @param flow_id Where, flow file md5 id, for multi version
 * @param start_time When, timestamp in milliseconds
 * @param end_time When, timestamp in milliseconds
 * @param choice What, flow procedure choices by branch
 * @param argument How, procedure input arguments
 * @param result How, procedure assign results
 * @param composite How, procedure compositor results
 * @param fault How, procedure fault
 * @param load How, procedure trace context variables
 * @param ancestor How, parent procedure id
 * @param descendants How, children procedure id list, such as include and schedule flow
 */
case class ProcedureView(
  id: String,
  trace_id: String,
  flow_name: String,
  flow_id: String,
  start_time: Long,
  end_time: Long,
  choice: Seq[String],
  argument: Map[String, String] = Map.empty,
  result: Map[String, String] = Map.empty,
  composite: Map[String, Composite] = Map.empty,
  fault: Map[String, Fault] = Map.empty,
  load: Array[Load] = Array.empty,
  ancestor: String = "",
  descendants: Array[String] = Array.empty
)
