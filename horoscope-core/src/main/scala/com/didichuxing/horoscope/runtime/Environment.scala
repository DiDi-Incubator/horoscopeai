/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.runtime

import com.didichuxing.horoscope.core.Flow
import com.didichuxing.horoscope.core.FlowRuntimeMessage.TraceVariableOrBuilder
import com.didichuxing.horoscope.runtime.experiment.ExperimentController

import scala.concurrent.Future

trait Environment {
  def shouldAccept(traceId: String): Boolean = true

  def getFlowByName(name: String): Flow

  def getController(flow: String): Option[ExperimentController]

  def getTraceContext(trace: String, keys: Array[String]): Future[Map[String, TraceVariableOrBuilder]]
}

