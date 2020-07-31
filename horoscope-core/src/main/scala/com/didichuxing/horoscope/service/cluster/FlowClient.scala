/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.cluster

import com.didichuxing.horoscope.core.Source
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.service.resource.Participant
import com.didichuxing.horoscope.util.Logging
import com.typesafe.config.ConfigRenderOptions
import com.didichuxing.horoscope.service.source.EventBusFactory
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.util.Try

/**
 * 集群版的数据源维护客户端，用于在线更新消息源
 */
class FlowClient(implicit ctx: ApplicationContext) extends Logging {

  implicit val config = ctx.config
  implicit val sourceFactories = ctx.sourceFactories
  implicit val zkClient = ctx.zkClient
  implicit val traceStore = ctx.traceStore
  implicit val resourceManager = ctx.resourceManager
  val httpSourceServer = ctx.httpSourceServer

  val zkPath = config.getString("horoscope.zookeeper.sources.path")
  val sources = mutable.Map[String, Source]()
  val zkSourceEnabled = Try(config.getBoolean("horoscope.zk-source-enabled")).getOrElse(true)

  private def getSourcePath(name: String): String = s"${zkPath}/${name}"

  /**
   * Source config in format:
   * {
   *   factory-name = "batchJsonKafka" // required
   *   source-name = "road_const"      // required
   *   flow-name = "/traffic/intelligence/road-open-hourly" // required
   *   parameter { // required
   *     kafka = {
   *       servers = "110.88.128.149:30372,10.85.128.81:30016"
   *       cluster-id = 28
   *       app-id = "appId_000328"
   *       password = "3FDf0MrEKqVB"
   *       topic = "traffic_mining_road_const"
   *       group = "intelligence-gateway-dev"
   *       max = 6
   *       concurrency = 1
   *      }
   *      rpc {
   *       port = 6880
    *      type = "grpc"
   *      }
   *      backpress {
   *         permits = 10
   *         timeout = 60
   *      }
   *    }
   * }
   */
  def registerSource(config: Config): Unit = {
    val sourceName = config.getString("source-name")
    val sourcePath = getSourcePath(sourceName)
    if (zkClient.create(sourcePath)) {
      if (zkClient.setData(sourcePath, config.root().render(ConfigRenderOptions.concise()))) {
        info(("msg", "register source success"), ("sourceName", sourceName))
      } else {
        error(("msg", "register source set data error"), ("sourceName", sourceName))
        throw new IllegalArgumentException(s"Can't register source ${sourceName}")
      }
    } else {
      error(("msg", "register source create node error"), ("sourceName", sourceName))
      throw new IllegalArgumentException(s"Can't register source ${sourceName}")
    }

  }

  /**
   * 移除一个数据源
   *
   * @param sourceName
   */
  def removeSource(sourceName: String): Unit = {
    val sourcePath = getSourcePath(sourceName)
    if (sources.contains(sourceName)) {
      throw new IllegalAccessException(s"Can't remove running source ${sourceName}, stop it first")
    }
    if (zkClient.exist(sourcePath)) {
      zkClient.delete(sourcePath)
    } else {
      throw new IllegalAccessException(s"${sourceName} not found")
    }
  }

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
    //zk source
    if (zkSourceEnabled && zkClient.exist(zkPath)) {
      for (sourceName <- zkClient.getChild(zkPath)) {
        startSource(sourceName)
      }
    }
    //config source
    try {
      info(("msg", "try to start config sources"))
      val sourceList = config.getConfigList("horoscope.sources")
      sourceList.foreach(sourceConfig => {
        val factoryName = sourceConfig.getString("factory-name")
        val sourceName = sourceConfig.getString("source-name")
        val flowName = sourceConfig.getString("flow-name")
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
    httpSourceServer.start(sources, this)
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
    httpSourceServer.stop()
  }

  /**
   * 当前所有注册的source
   *
   * @return
   */
  def listAllRegistered(): Seq[String] = {
    val list = new ArrayBuffer[String]()
    if (zkClient.exist(zkPath)) {
      for (sourceName <- zkClient.getChild(zkPath)) {
        val data = zkClient.getData(getSourcePath(sourceName))
        if (data.isDefined) {
          list += data.get
        }
      }
    }
    list.toList
  }

  def listAllRunning(): Seq[String] = {
    sources.map(_._1).toSeq
  }

  /**
   * 返回当前集群中的所有参与者
   *
   * @return
   */
  def getParticipants(): Seq[Participant] = {
    resourceManager.getParticipants()
  }

  def getLocal(): Participant = {
    resourceManager.local()
  }
}
