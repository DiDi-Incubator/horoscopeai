/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.exec

import java.util.concurrent.ConcurrentHashMap
import java.util.function.Function

import com.didichuxing.horoscope.core.FlowDslMessage.CompositorDef
import com.didichuxing.horoscope.core.FlowRuntimeMessage.TraceVariableOrBuilder
import com.didichuxing.horoscope.core._
import com.didichuxing.horoscope.runtime.Environment
import com.didichuxing.horoscope.runtime.expression.{BuiltIn, DefaultBuiltIn}
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.util.Logging

import scala.concurrent.Future
import scala.language.implicitConversions

class LocalExecutorEnvironment(implicit ctx: ApplicationContext) extends Environment with Logging {

  implicit val flowStore = ctx.flowStore
  implicit val traceStore = ctx.traceStore
  implicit val compositorFactories = ctx.compositorFactories
  implicit val builtIn = ctx.builtIn
  private val flowCache = new ConcurrentHashMap[String, Flow]()

  implicit def buildCompositor(definition: CompositorDef): Compositor = {
    val factoryName = definition.getFactory
    val factory = compositorFactories.get(factoryName)
    if (factory.isDefined) {
      factory.get.create(definition.getContent)
    } else {
      null
    }
  }

  implicit val builtInFun: BuiltIn = {
    if (DefaultBuiltIn.defaultBuiltin != this.builtIn) {
      DefaultBuiltIn.defaultBuiltin.toBuilder().mergeFrom(this.builtIn).build()
    } else {
      DefaultBuiltIn.defaultBuiltin
    }
  }

  override def getFlowByName(name: String): Option[Flow] = {
    flowStore.getFlowByName(name) match {
      case Some(flowDef) =>
        try {
          val flow = flowCache.computeIfAbsent(name, new Function[String, Flow]() {
            override def apply(t: String): Flow = {
              info(("msg", "create flow"), ("name", name))
              Flow(flowDef)(buildCompositor _, builtInFun)
            }
          })
          Option(flow)
        } catch {
          case ex: Throwable =>
            ex.printStackTrace()
            error(("msg", "create flow error"), ("name", name), ("ex", ex.getCause), ("detail", ex.toString))
            None
        }
      case None =>
        error(("msg", "flow not found"), ("name", name))
        None
    }
  }

  override def getTraceContext(trace: String, keys: Array[String]): Future[Map[String, TraceVariableOrBuilder]] = {
    traceStore.getContext(trace, keys)
  }

  override def shouldAccept(traceId: String): Boolean = {
    true
  }
}
