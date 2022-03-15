/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.runtime

import com.didichuxing.horoscope.core.{ExperimentController, Flow}
import com.didichuxing.horoscope.core.FlowRuntimeMessage.TraceVariableOrBuilder
import com.didichuxing.horoscope.runtime.expression.BuiltIn
import com.didichuxing.horoscope.util.FlowGraph

import scala.concurrent.Future

trait Environment {
  def shouldAccept(traceId: String): Boolean = true

  def getFlowByName(name: String): Flow

  def getBuiltIn(): BuiltIn

  def getFlowGraph(): FlowGraph

  def getController(flow: String): Seq[ExperimentController]

  def getTraceContext(trace: String, keys: Array[String]): Future[Map[String, TraceVariableOrBuilder]]
}

