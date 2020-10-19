/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.exec

import com.didichuxing.horoscope.core.FlowDslMessage.CompositorDef
import com.didichuxing.horoscope.core.FlowRuntimeMessage.TraceVariableOrBuilder
import com.didichuxing.horoscope.core._
import com.didichuxing.horoscope.runtime.Environment
import com.didichuxing.horoscope.runtime.expression.{BuiltIn, DefaultBuiltIn}
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.util.Logging

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.language.implicitConversions

class LocalExecutorEnvironment(implicit ctx: ApplicationContext) extends Environment with Logging {
  implicit val builtIn: BuiltIn = ctx.builtIn

  implicit def buildCompositor(definition: CompositorDef): Compositor = {
    val factoryName = definition.getFactory
    val factory = compositorFactories.get(factoryName)
    if (factory.isDefined) {
      factory.get.create(definition.getContent)
    } else {
      null
    }
  }

  private val compositorFactories: Map[String, CompositorFactory] = ctx.compositorFactories
  private val flowCache = TrieMap.empty[String, Flow]

  override def getFlowByName(name: String): Flow = {
    import ctx.flowStore

    try {
      val flowDef = flowStore.getFlowByName(name)
      val flow = flowCache.getOrElseUpdate(name, Flow(flowDef))
      if (flow.id == flowDef.getId) {
        flow
      } else {
        val newFlow = Flow(flowDef)
        flowCache.update(name, newFlow)
        newFlow
      }
    } catch {
      case cause: Throwable =>
        error(("msg", "create flow error"), ("name", name), ("ex", cause.getCause), ("detail", cause.toString))
        flowCache.remove(name)
        throw cause
    }
  }

  override def getTraceContext(trace: String, keys: Array[String]): Future[Map[String, TraceVariableOrBuilder]] = {
    ctx.traceStore.getContext(trace, keys)
  }

  override def shouldAccept(traceId: String): Boolean = {
    true
  }
}
