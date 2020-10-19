/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.local

import akka.actor.ActorSystem
import com.didichuxing.horoscope.core._
import com.didichuxing.horoscope.core.OdsLogger
import com.didichuxing.horoscope.logging.OdsLoggerFactory
import com.didichuxing.horoscope.runtime.FlowExecutorImpl
import com.didichuxing.horoscope.runtime.expression.DefaultBuiltIn
import com.didichuxing.horoscope.service.exec.LocalExecutorEnvironment
import com.didichuxing.horoscope.service.scheduler._
import com.didichuxing.horoscope.runtime.expression.BuiltIn
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.service.api.HttpServer
import com.didichuxing.horoscope.service.source._
import com.didichuxing.horoscope.service.storage.{DefaultFlowStore, DefaultTraceStore}
import com.didichuxing.horoscope.util.Logging
import com.typesafe.config.Config

/**
 * 本部版本的服务构建
 */
class LocalServiceBuilder extends ServiceBuilder with Logging {

  implicit val ctx = ApplicationContext()

  override def withFlowStore(flowStore: FlowStore): LocalServiceBuilder.this.type = {
    ctx.withFlowStore(flowStore)
    this
  }

  override def withTraceStore(traceStore: TraceStore): LocalServiceBuilder.this.type = {
    ctx.withTraceStore(traceStore)
    this
  }

  override def withSourceFactory(name: String, factory: SourceFactory): LocalServiceBuilder.this.type = {
    ctx.withSourceFactory(name, factory)
    this
  }

  override def withCompositorFactory(name: String, factory: CompositorFactory): LocalServiceBuilder.this.type = {
    ctx.withCompositorFactory(name, factory)
    this
  }

  override def withConfig(config: Config): LocalServiceBuilder.this.type = {
    ctx.withConfig(config)
    this
  }

  override def withActorSystem(system: ActorSystem): LocalServiceBuilder.this.type = {
    ctx.withActorSystem(system)
    this
  }

  override def withTimeTrigger(timeTrigger: TimeTrigger): LocalServiceBuilder.this.type = {
    ctx.withTimeTrigger(timeTrigger)
    this
  }

  override def withBuiltin(builtin: BuiltIn): this.type = {
    ctx.withBuiltin(builtin)
    this
  }

  override def withExecutionContext(executor: SourceExecutionContext): this.type = {
    ctx.withSourceExecutionContext(executor)
    this
  }

  override def withOdsLogger(odsLogger: OdsLogger): this.type = {
    ctx.withOdsLogger(odsLogger)
    this
  }

  def checkContext(): Unit = {
    assert(ctx.config != null, "config is null")
    if (ctx.flowStore == null) {
      ctx.withFlowStore(new DefaultFlowStore)
      info("horoscope default init flow store")
    }
    if (ctx.traceStore == null) {
      ctx.withTraceStore(new DefaultTraceStore)
      info("horoscope default init trace store")
    }
    if (ctx.system == null) {
      ctx.withActorSystem(ActorSystem("HoroscopeSystem", ctx.config))
      info("horoscope init default actor system HoroscopeSystem")
    }
    if (ctx.timeTrigger == null) {
      ctx.withTimeTrigger(new DefaultTimeTrigger(ctx.config))
    }
    if (ctx.builtIn == null) {
      ctx.withBuiltin(DefaultBuiltIn.defaultBuiltin)
    }
    if (ctx.sourceExecutionContext == null) {
      ctx.withSourceExecutionContext(DefaultSourceExecutionContext(ctx.config))
    }
    if (ctx.odsLogger == null) {
      info("use local ods logger")
      ctx.withOdsLogger(OdsLoggerFactory.newLocalLogger(ctx.config))
    }
    if (ctx.multiScheduler == null) {
      ctx.withMultiScheduler(new DefaultMultiScheduler())
    }
  }

  def build(): FlowManager = {
    checkContext()
    ctx.withFlowExecutor(new FlowExecutorImpl(ctx.config, ctx.system, new LocalExecutorEnvironment))
    ctx.withScheduler(new MemoryScheduler)
    ctx.withHttpServer(new HttpServer)
    new FlowManager
  }

}
