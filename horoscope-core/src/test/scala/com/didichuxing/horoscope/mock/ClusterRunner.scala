/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.mock

import com.didichuxing.horoscope.core.{FlowStore, Horoscope, Sources}
import com.didichuxing.horoscope.runtime.expression.{DefaultBuiltIn, ZookeeperJythonBuiltIn}
import com.didichuxing.horoscope.service.cluster.FlowClient
import com.didichuxing.horoscope.service.resource.ZkClient
import com.didichuxing.horoscope.service.source.EventBuilders
import com.didichuxing.horoscope.service.storage.{GitFileStore, GitFlowStore, RedisTraceStore, ZookeeperConfigStore}
import com.didichuxing.horoscope.util.Constants.SCH_SOURCE_FACTORY
import com.didichuxing.horoscope.util.{Logging, SystemLog}
import com.typesafe.config.{Config, ConfigFactory}

class ClusterRunner extends Logging {
  def run(config: Config): FlowClient = {
    SystemLog.create()
    info("cluster horoscope init...")
    //init traceStore
    val zkClient = new ZkClient(config)
    val traceStore = new RedisTraceStore

    val configStore = new ZookeeperConfigStore(zkClient.configCurator(), zkClient)
    val fileStore = new GitFileStore(config)

    val flowStore = GitFlowStore.newBuilder()
      .withCompositorFactory("default", new MockCompositorFactory)
      .withConfigStore(configStore)
      .withFileStore(fileStore)
      .build()

    traceStore.start(config)
    //init service builder
    val serviceBuilder = Horoscope
      .newClusterService()
      .withConfig(config)
      .withBuiltIn(flowStore.getBuiltIn)
      .withZkClient(zkClient)
      .withFlowStore(flowStore)
      .withConfigStore(configStore)
      .withFileStore(fileStore)
      .withConfigStore(configStore)
      .withTraceStore(traceStore)
      .withSourceFactory("batchJsonKafka", Sources.kafka(EventBuilders.sourceEventBuilder()))
    //start serverice
    info("horoscope begin start service")
    val worker = serviceBuilder.buildWorker()
    worker.startService()
    //client
    info("horoscope begin start sources")
    val client = serviceBuilder.builderClient()
    client.startAllSources()
    info("horoscope init complete")
    //release service resource
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = {
        client.stopAllSources()
        worker.stopService()
        traceStore.stop()
      }
    })
    client
  }
}

object Cluster2552 extends App {
  new ClusterRunner().run(ConfigFactory.load("application-remoting-2552.conf"))
}

object Cluster2553 extends App {
  new ClusterRunner().run(ConfigFactory.load("application-remoting-2553.conf"))
}

object Cluster2554 extends App {
  new ClusterRunner().run(ConfigFactory.load("application-remoting-2554.conf"))
}
