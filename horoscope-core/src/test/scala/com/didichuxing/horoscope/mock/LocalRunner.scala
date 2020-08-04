/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.mock

import com.didichuxing.horoscope.core.{Horoscope, Sources}
import com.didichuxing.horoscope.service.local.FlowManager
import com.didichuxing.horoscope.service.source.{EventBuilders, HttpSourceFactory}
import com.didichuxing.horoscope.util.Constants.SCH_SOURCE_FACTORY
import com.didichuxing.horoscope.util.{Logging, SystemLog}
import com.typesafe.config.{Config, ConfigFactory}

class LocalRunner extends Logging {

  var flowManager: FlowManager = _

  def run(config: Config): Unit = {
    //初始化系统日志
    SystemLog.create()
    info("local horoscope init...")
    //构造flowManager
    flowManager = Horoscope.newLocalService()
      .withConfig(config)
      .withCompositorFactory("default", new MockCompositorFactory)
      .withSourceFactory("batchJsonKafka", Sources.kafka(EventBuilders.sourceEventBuilder()))
      .withSourceFactory(SCH_SOURCE_FACTORY, Sources.scheduler(EventBuilders.schedulerSourceEventBuilder()))
      .withSourceFactory("jsonHttp", Sources.http(EventBuilders.jsonEventBuilder()))
      .withSourceFactory("scheduleHttp", new HttpSourceFactory())
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
  HttpSourceClient.postEvent("http://localhost:5880/schedule/debug/_/now/v2/hello")
  HttpSourceClient.postEvent("http://localhost:5880/schedule/debug/_/now/v2/hello")
  HttpSourceClient.postEvent("http://localhost:5880/acheduleAsync/debug/_/now/v2/hello")
  HttpSourceClient.stop()
  runner.stop
}
