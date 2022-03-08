package com.didichuxing.horoscope.examples

import com.didichuxing.horoscope.core.{Horoscope, Sources}
import com.didichuxing.horoscope.service.local.FlowManager
import com.didichuxing.horoscope.service.storage.{GitFlowStore, LocalConfigStore, LocalFileStore}
import com.didichuxing.horoscope.util.{Logging, SystemLog}
import com.typesafe.config.{Config, ConfigFactory}

class DemoLocalService extends Logging {

  var flowManager: FlowManager = _
  val config: Config = ConfigFactory.load(Thread.currentThread().getContextClassLoader, "application")

  def run(): Unit = {
    SystemLog.create()
    info("local service init")
    val flowStore = GitFlowStore.newBuilder()
      .withFileStore(new LocalFileStore(config))
      .withConfigStore(new LocalConfigStore)
      .build()
    flowManager = Horoscope.newLocalService()
      .withConfig(config)
      .withBuiltIn(flowStore.getBuiltIn)
      .withFlowStore(flowStore)
      .withSourceFactory("httpSource", Sources.http())
      .build()
    info("start service")
    flowManager.startService()
    info("start sources")
    flowManager.startSources()
    info("service already started")
  }
}

object DemoLocalService {
  def main(args: Array[String]): Unit = {
    val localService = new DemoLocalService()
    localService.run()
  }
}
