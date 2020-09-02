/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service

import akka.actor.ActorSystem
import com.didichuxing.horoscope.core.{CompositorFactory, FlowStore, SourceFactory, TraceStore}
import com.didichuxing.horoscope.logging.ods.OdsLogger
import com.didichuxing.horoscope.runtime.FlowExecutor
import com.didichuxing.horoscope.runtime.expression.BuiltIn
import com.didichuxing.horoscope.service.api.HttpServer
import com.didichuxing.horoscope.service.resource.{ResourceManager, ZkClient}
import com.didichuxing.horoscope.service.scheduler.{MultiScheduler, Scheduler, TimeTrigger}
import com.didichuxing.horoscope.service.source.SourceExecutionContext
import com.typesafe.config.Config

import scala.collection.mutable

/**
 * horoscope app context
 */
//scalastyle:off
class ApplicationContext {

  private var fe: FlowExecutor = _
  private var sch: Scheduler = _
  private var msch: MultiScheduler = _
  private var cfg: Config = _
  private val sf: mutable.Map[String, SourceFactory] = mutable.Map[String, SourceFactory]()
  private var ts: TraceStore = _
  private var as: ActorSystem = _
  private var tt: TimeTrigger = _
  private var sec: SourceExecutionContext = _
  private val cf: mutable.Map[String, CompositorFactory] = mutable.Map[String, CompositorFactory]()
  private var bi: BuiltIn = _
  private var fs: FlowStore = _
  private var rm: ResourceManager = _
  private var zkc: ZkClient = _
  private var hss: HttpServer = _
  private var ods: OdsLogger = _

  def withResourceManager(resourceManager: ResourceManager): this.type = {
    rm = resourceManager
    this
  }

  def withFlowExecutor(flowExecutor: FlowExecutor): this.type = {
    fe = flowExecutor
    this
  }

  def withScheduler(scheduler: Scheduler): this.type = {
    sch = scheduler
    this
  }

  def withMultiScheduler(scheduler: MultiScheduler): this.type = {
    msch = scheduler
    this
  }

  def withFlowStore(flowStore: FlowStore): this.type = {
    fs = flowStore
    this
  }

  def withTraceStore(traceStore: TraceStore): this.type = {
    ts = traceStore
    this
  }

  def withSourceFactory(name: String, factory: SourceFactory): this.type = {
    sf.put(name, factory)
    this
  }

  def withCompositorFactory(name: String, factory: CompositorFactory): this.type = {
    cf.put(name, factory)
    this
  }

  def withConfig(config: Config): this.type = {
    cfg = config
    this
  }

  def withActorSystem(system: ActorSystem): this.type = {
    as = system
    this
  }

  def withTimeTrigger(timeTrigger: TimeTrigger): this.type = {
    tt = timeTrigger
    this
  }

  def withBuiltin(builtin: BuiltIn): this.type = {
    bi = builtin
    this
  }

  def withSourceExecutionContext(sourceExecutionContext: SourceExecutionContext): this.type = {
    sec = sourceExecutionContext
    this
  }

  def withZKClient(zkClient: ZkClient): this.type = {
    zkc = zkClient
    this
  }

  def withHttpServer(httpServer: HttpServer): this.type = {
    hss = httpServer
    this
  }

  def withOdsLogger(odsLogger: OdsLogger): this.type = {
    ods = odsLogger
    this
  }

  def config: Config = cfg

  def sourceFactories: Map[String, SourceFactory] = sf.toMap

  def flowExecutor: FlowExecutor = fe

  def scheduler: Scheduler = sch

  def multiScheduler: MultiScheduler = msch

  def system: ActorSystem = as

  def timeTrigger: TimeTrigger = tt

  def sourceExecutionContext: SourceExecutionContext = sec

  def traceStore: TraceStore = ts

  def compositorFactories: Map[String, CompositorFactory] = cf.toMap

  def builtIn: BuiltIn = bi

  def flowStore: FlowStore = fs

  def resourceManager: ResourceManager = rm

  def zkClient: ZkClient = zkc

  def httpServer: HttpServer = hss

  def odsLogger: OdsLogger = ods
}

object ApplicationContext {
  def apply(): ApplicationContext = new ApplicationContext()
}
