/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.mock

import com.didichuxing.horoscope.core.{FlowStore, Horoscope, Sources}
import com.didichuxing.horoscope.service.local.FlowManager
import com.didichuxing.horoscope.service.source.{EventBuilders, HttpSourceFactory}
import com.didichuxing.horoscope.service.storage.{GitFlowStore, LocalConfigStore, LocalFileStore}
import com.didichuxing.horoscope.util.{Logging, SystemLog}
import com.typesafe.config.{Config, ConfigFactory}

class LocalRunner extends Logging {

  var flowManager: FlowManager = _

  def run(config: Config): Unit = {
    //初始化系统日志
    SystemLog.create()
    info("local horoscope init...")
    //构造flowManager

    val flowStore = GitFlowStore.newBuilder()
      .withFileStore(new LocalFileStore(null))
      .withConfigStore(new LocalConfigStore)
      .build()

    flowManager = Horoscope.newLocalService()
      .withConfig(config)
      .withBuiltIn(flowStore.getBuiltIn)
      .withFlowStore(flowStore)
      .withSourceFactory("batchJsonKafka", Sources.kafka(EventBuilders.sourceEventBuilder()))
      .withSourceFactory("httpSource", Sources.http())
      .build()


    //启动主服务
    info("horoscope begin start service")
    flowManager.startService()
    //启动外部source
    info("horoscope begin start sources")
    flowManager.startSources()
    info("horoscope init complete")
  }

  def stop(): Unit = {
    flowManager.stopAllSources()
    flowManager.stopService()
  }
}

object LocalDemo extends App {
  val runner = new LocalRunner()
  runner.run(ConfigFactory.load("application-local.conf"))
  HttpSourceClient.postEvent("http://localhost:8062/api/schedule/debug/_/now/v2/hello")
  HttpSourceClient.postEvent("http://localhost:8062/api/schedule/debug/_/now/v2/hello")
  HttpSourceClient.postEvent("http://localhost:8062/api/acheduleAsync/debug/_/now/v2/hello")
  HttpSourceClient.stop()
  runner.stop
}
