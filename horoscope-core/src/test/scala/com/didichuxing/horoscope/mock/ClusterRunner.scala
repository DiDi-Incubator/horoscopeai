/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.mock

import com.didichuxing.horoscope.core.{Horoscope, Sources}
import com.didichuxing.horoscope.service.cluster.FlowClient
import com.didichuxing.horoscope.service.resource.ZkClient
import com.didichuxing.horoscope.service.source.{EventBuilders, HttpSourceFactory}
import com.didichuxing.horoscope.service.storage.{HBaseTraceStore, RedisTraceStore, ZookeeperFlowStore}
import com.didichuxing.horoscope.util.Constants.SCH_SOURCE_FACTORY
import com.didichuxing.horoscope.util.{Logging, SystemLog}
import com.typesafe.config.{Config, ConfigFactory}

class ClusterRunner extends Logging {
  def run(config: Config): FlowClient = {
    SystemLog.create()
    info("cluster horoscope init...")
    //init traceStore
    val zkClient = new ZkClient(config)
    val flowStore = new ZookeeperFlowStore(zkClient.flowsCurator())
    val traceStore = new RedisTraceStore
    traceStore.start(config)
    //init service builder
    val serviceBuilder = Horoscope
      .newClusterService()
      .withConfig(config)
      .withZkClient(zkClient)
      .withFlowStore(flowStore)
      .withTraceStore(traceStore)
      .withCompositorFactory("default", new MockCompositorFactory)
      .withSourceFactory("batchJsonKafka", Sources.kafka(EventBuilders.sourceEventBuilder()))
      .withSourceFactory(SCH_SOURCE_FACTORY, Sources.scheduler(EventBuilders.schedulerSourceEventBuilder()))

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
