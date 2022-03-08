/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.local

import com.didichuxing.horoscope.core.Source
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.service.source.EventBusFactory
import com.didichuxing.horoscope.util.Logging
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/**
 * 本地版本的星盘实现
 */
class FlowManager(implicit ctx: ApplicationContext) extends Logging {

  implicit val system = ctx.system
  implicit val config = ctx.config
  implicit val sourceFactories = ctx.sourceFactories
  implicit val traceStore = ctx.traceStore
  implicit val flowExecutor = ctx.flowExecutor
  implicit val sec = ctx.sourceExecutionContext
  val sources = mutable.Map[String, Source]()

  def startService(): Unit = {
    flowExecutor.start()
  }

  def stopService(): Unit = {
    flowExecutor.stop()
    Await.result(system.terminate(), 10 seconds)
    sec.stop()
  }

  /**
   * 启动一个外部数据源
   *
   * @param factoryName
   * @param sourceConfig 数据源配置，不是全局配置
   * @param sourceName
   * @param flowName
   */
  def startSource(factoryName: String, sourceConfig: Config)(sourceName: String, flowName: String): Unit = {
    val sourceFactory = sourceFactories.get(factoryName)
    if (sourceFactory.isDefined) {
      val source = sourceFactory.get.newSource(sourceConfig)
      val parameter = Try(sourceConfig.getConfig("parameter")).getOrElse(ConfigFactory.empty())
      source.start(EventBusFactory.newEventBus(sourceName, flowName, parameter))
      info(("msg", s"$sourceName has start"))
      sources.put(sourceName, source)
    } else {
      error(("msg", s"$factoryName not found"))
    }
  }

  /**
   * 启动外部数据源
   */
  def startSources(): Unit = {
    //加载source-配置文件
    val sourceList = config.getConfigList("horoscope.sources")
    sourceList.foreach(sourceConfig => {
      val factoryName = sourceConfig.getString("factory-name")
      val sourceName = sourceConfig.getString("source-name")
      val flowName = Try(sourceConfig.getString("flow-name")).getOrElse("dead_letter")
      startSource(factoryName, sourceConfig)(sourceName, flowName)
    })
    //start http source server
    ctx.httpServer.start(sources)
  }

  /**
   * 停止一个数据源
   *
   * @param sourceName
   */
  def stopSource(sourceName: String): Unit = {
    val source = sources.get(sourceName)
    if (source.isDefined) {
      source.get.stop()
      info(("msg", s"$sourceName has stop"))
      sources.remove(sourceName)
    } else {
      error(("msg", s"$sourceName not found"))
    }
  }

  /**
   * 停止所有数据源
   */
  def stopAllSources(): Unit = {
    //stop http source server
    ctx.httpServer.stop()
    sources.foreach(source => {
      source._2.stop()
      info(("msg", s"${source._1} has stop"))
    })
  }
}
