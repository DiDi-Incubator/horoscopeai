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

case class ProcedureView(
  trace_id: String,
  id: String,
  flow_name: String,
  start_time: Long,
  end_time: Long,
  choice: Seq[String],
  argument: Map[String, String],
  result: Map[String, String],
  composite: Map[String, Composite],
  fault: Map[String, Fault]
)
