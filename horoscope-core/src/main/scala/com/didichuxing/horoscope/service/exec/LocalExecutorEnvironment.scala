/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.exec

import com.didichuxing.horoscope.core.FlowRuntimeMessage.TraceVariableOrBuilder
import com.didichuxing.horoscope.core.{ExperimentController, _}
import com.didichuxing.horoscope.runtime.Environment
import com.didichuxing.horoscope.runtime.expression.BuiltIn
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.util.{FlowGraph, Logging}

import scala.concurrent.Future
import scala.language.implicitConversions

class LocalExecutorEnvironment(implicit ctx: ApplicationContext) extends Environment with Logging {

  override def getFlowByName(name: String): Flow = {
    ctx.flowStore.getFlow(name)
  }

  override def getFlowGraph(): FlowGraph = {
    ctx.flowStore.getFlowGraph
  }

  override def getController(flow: String): Seq[ExperimentController] = {
    ctx.flowStore.getController(flow)
  }

  override def getBuiltIn(): BuiltIn = {
    ctx.flowStore.getBuiltIn
  }

  override def getTraceContext(trace: String, keys: Array[String]): Future[Map[String, TraceVariableOrBuilder]] = {
    ctx.traceStore.getContext(trace, keys)
  }

  override def shouldAccept(traceId: String): Boolean = {
    true
  }
}
