/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.cluster

import com.didichuxing.horoscope.core.Source
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.util.Logging
import com.didichuxing.horoscope.service.source.EventBusFactory
import com.didichuxing.horoscope.service.storage.SourceListener
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.util.Try

/**
 * 集群版的数据源维护客户端，用于在线更新消息源
 */
class FlowClient(implicit ctx: ApplicationContext) extends SourceListener with Logging {

  implicit val config = ctx.config
  implicit val sourceFactories = ctx.sourceFactories
  implicit val zkClient = ctx.zkClient
  val zkPath = zkClient.getSourcePath()
  val sources = mutable.Map[String, Source]()

  private def getSourcePath(name: String): String = s"${zkPath}/${name}"

  /**
   * 启动一个外部数据源
   *
   * @param factoryName
   * @param sourceConfig 数据源参数
   * @param sourceName
   * @param flowName
   */
  def startSource(factoryName: String, sourceConfig: Config)(sourceName: String, flowName: String): Unit = {
    if (sources.contains(sourceName)) {
      error(("msg", s"do not repeatedly start '$sourceName'"))
      throw new IllegalAccessException(s"do not repeatedly start '$sourceName'")
    } else {
      val params = sourceConfig.getConfig("parameter")
      if (!Try(params.getBoolean("disabled")).getOrElse(false)) {
        val sourceFactory = sourceFactories.get(factoryName)
        if (sourceFactory.isDefined) {
          val source = sourceFactory.get.newSource(sourceConfig)
          source.start(EventBusFactory.newEventBus(sourceName, flowName, params))
          sources.put(sourceName, source)
          info(("msg", s"$sourceName started successfully"))
        } else {
          error(("msg", s"$factoryName not found"))
          throw new IllegalAccessException(s"${sourceName} factory ${factoryName} not found")
        }
      } else {
        error(s"Can't start disabled source '$sourceName'")
      }
    }
  }

  def startSource(sourceName: String): Unit = {
    val sourcePath = getSourcePath(sourceName)
    if (zkClient.exist(zkPath)) {
      val data = zkClient.getData(sourcePath)
      if (data.isDefined) {
        val config = ConfigFactory.parseString(data.get)
        val sourceName = config.getString("source-name")
        val factoryName = config.getString("factory-name")
        val flowName = config.getString("flow-name")
        startSource(factoryName, config)(sourceName, flowName)
      } else {
        if (Try(startConfigSource(sourceName)).isFailure) {
          error(("msg", "not found any source"), ("zkPath", zkPath))
          throw new IllegalAccessException(s"${sourceName} not found on zk ${zkPath}")
        }
      }
    } else {
      if (Try(startConfigSource(sourceName)).isFailure) {
        throw new IllegalAccessException(s"Can't find ${sourceName} to start on zk ${sourcePath}")
      }
    }
  }

  private def startConfigSource(confSource: String): Unit = {
    info(("msg", s"try to start config source $confSource"))
    val sourceList = config.getConfigList("horoscope.sources")
    sourceList.foreach(sourceConfig => {
      val factoryName = sourceConfig.getString("factory-name")
      val sourceName = sourceConfig.getString("source-name")
      val flowName = sourceConfig.getString("flow-name")
      if (confSource.equals(sourceName) && !sources.contains(sourceName)) {
        startSource(factoryName, sourceConfig)(sourceName, flowName)
      }
    })
  }

  /**
   * 启动zk注册的所有数据源
   */
  def startAllSources(): Unit = {
    //config source
    try {
      info(("msg", "try to start config sources"))
      val sourceList = config.getConfigList("horoscope.sources")
      sourceList.foreach(sourceConfig => {
        val factoryName = sourceConfig.getString("factory-name")
        val sourceName = sourceConfig.getString("source-name")
        val flowName = Try(sourceConfig.getString("flow-name")).getOrElse("dead_letter")
        if (!sources.contains(sourceName)) {
          startSource(factoryName, sourceConfig)(sourceName, flowName)
        }
      })
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        warn(("msg", ex.getMessage))
    }
    //start http source server
    ctx.httpServer.start(sources, this)
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
      throw new IllegalAccessException(s"${sourceName} not found")
    }
  }

  /**
   * 停止所有数据源
   */
  def stopAllSources(): Unit = {
    sources.foreach(source => {
      stopSource(source._1)
    })
    //停止http source server
    ctx.httpServer.stop()
  }

  override def onRegister(source: Config): Unit = {
    debug(s"on source add: $source")
    val sourceName = source.getString("source-name")
    val disabled = Try(source.getBoolean("parameter.disabled")).getOrElse(true)
    if(!disabled) {
      startSource(sourceName)
    } else {
      debug(s"source $sourceName disabled")
    }
  }

  override def onRemove(sourceName: String): Unit = {
    debug(s"on source delete: $sourceName")
    if(sources.contains(sourceName)) {
      stopSource(sourceName)
    } else {
      debug(s"source $sourceName not start")
    }
  }

  override def onUpdate(sourceName: String, source: Config): Unit = {
    debug(s"on source update: $source")
    val disabled = Try(source.getBoolean("parameter.disabled")).getOrElse(true)
    if(sources.contains(sourceName)) {
      stopSource(sourceName)
    }
    if(!disabled) {
      startSource(sourceName)
    }
  }
}
