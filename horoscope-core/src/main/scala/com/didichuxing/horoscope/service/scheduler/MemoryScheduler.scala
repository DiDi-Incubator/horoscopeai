/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.scheduler

import com.didichuxing.horoscope.core.{FlowRuntimeMessage, SourceFactory}
import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowEvent
import com.didichuxing.horoscope.runtime.FlowExecutor
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.service.resource.ResourceManager
import com.didichuxing.horoscope.service.source.{SchedulerEventBus, SchedulerSource}
import com.didichuxing.horoscope.util.Constants._
import com.didichuxing.horoscope.util.Utils._
import com.didichuxing.horoscope.util.Logging
import com.typesafe.config.Config

import scala.collection.mutable.ListBuffer

class MemoryScheduler(implicit ctx: ApplicationContext) extends Scheduler with Logging {

  implicit val config = ctx.config
  implicit val traceStore = ctx.traceStore
  implicit val ec = ctx.sourceExecutionContext.getExecutionContext()
  val db = ListBuffer[FlowEvent]()
  var delaySource: SchedulerSource = _

  override def start(flowExecutor: FlowExecutor, sourceFactories: Map[String, SourceFactory]): Unit = {
    val schSourceFactory = sourceFactories.get(SCH_SOURCE_FACTORY)
    if (schSourceFactory.isDefined) {
      val delayConfig = config.getConfig("horoscope.scheduler.delay")
      delaySource = startSchedulerSource(flowExecutor, schSourceFactory.get, delayConfig, SCH_DELAY_SOURCE)
      debug(("msg", "scheduler start success"))
    }
  }

  override def stop(): Unit = {
    if (delaySource != null) {
      delaySource.stop()
    }
  }

  override def onTimeSpanSwitch(timestamp: Long): Unit = {
    debug(("msg", "onTimeSpanSwitch"), ("timestamp", timestamp))
    val events = db.filter(event => {
      timestamp > event.getScheduledTimestamp
    })
    for (event <- events) {
      delaySource.push(List(event.toByteArray))
    }
    db --= events
  }

  override def goto(event: FlowRuntimeMessage.FlowEvent): Unit = {
    if (event.hasScheduledTimestamp && event.getScheduledTimestamp > 0) {
      db.append(event)
    } else {
      //immediately process
    }
  }

  /**
   * 启动一个内部的scheduler数据源
   */
  private def startSchedulerSource(flowExecutor: FlowExecutor, sourceFactory: SourceFactory, params: Config,
                                   sourceName: String): SchedulerSource = {
    val source = sourceFactory.newSource(params)
    source.start(new SchedulerEventBus(schStoreCol(config), params))
    info(("msg", s"$sourceName has start"))
    source.asInstanceOf[SchedulerSource]
  }

  override def recovery(resourceManager: ResourceManager): Unit = {
    debug(("msg", "scheduler recovery"))
    val flowEvents = traceStore.getEventsBySource(schStoreCol(config), 0, 0)
    for (event <- flowEvents.map(f => f.asInstanceOf[FlowEvent])) {
      if (event.hasScheduledTimestamp && event.getScheduledTimestamp > 0) {
        db.append(event)
      } else {
        //immediately process
      }
    }
  }
}
