/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.cluster

import com.didichuxing.horoscope.runtime.FlowExecutorImpl
import com.didichuxing.horoscope.service.exec.ClusterExecutorEnvironment
import com.didichuxing.horoscope.service.local.LocalServiceBuilder
import com.didichuxing.horoscope.service.resource.{DefaultResourceManager, ZkClient}
import com.didichuxing.horoscope.service.scheduler._
import com.didichuxing.horoscope.service.source.{DefaultSourceExecutionContext, HttpSourceServer}
import com.didichuxing.horoscope.util.Utils._
import com.didichuxing.horoscope.util.Logging
import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Try

/**
 * 集群版的服务构建
 */
class ClusterServiceBuilder extends LocalServiceBuilder with Logging {

  override def withConfig(config: Config): ClusterServiceBuilder.this.type = {
    val hostname = "akka.remote.netty.tcp.hostname"
    val akkaConf = if (Try(config.getString(hostname)).getOrElse("") != "") {
      config
    } else {
      val localIp = guessLocalIp()
      ConfigFactory.parseString(s"$hostname = $localIp").withFallback(config)
    }
    ctx.withConfig(akkaConf)
    this
  }

  def buildWorker(): FlowWorker = {
    checkContext()
    ctx.withZKClient(new ZkClient)
    ctx.withScheduler(new RocksDBScheduler)
    ctx.withResourceManager(new DefaultResourceManager)
    ctx.withFlowExecutor(new FlowExecutorImpl(ctx.config, ctx.system, new ClusterExecutorEnvironment))
    ctx.withHttpSourceServer(new HttpSourceServer)
    new FlowWorker
  }

  def builderClient(): FlowClient = {
    assert(ctx != null, "application context is null")
    new FlowClient
  }
}
