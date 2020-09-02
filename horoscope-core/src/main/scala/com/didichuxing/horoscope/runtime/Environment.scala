/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.runtime

import com.didichuxing.horoscope.core.Flow
import com.didichuxing.horoscope.core.FlowRuntimeMessage.TraceVariableOrBuilder

import scala.concurrent.Future

trait Environment {
  def shouldAccept(traceId: String): Boolean = true

  def getFlowByName(name: String): Flow

  def getTraceContext(trace: String, keys: Array[String]): Future[Map[String, TraceVariableOrBuilder]]
}

