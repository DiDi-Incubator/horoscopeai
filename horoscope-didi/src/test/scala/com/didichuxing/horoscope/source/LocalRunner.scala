/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.source

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.didichuxing.horoscope.apollo.{ApolloConfigCompositorFactory, ApolloService, ApolloToggleCompositorFactory}
import com.didichuxing.horoscope.compositor.{MLFlowCompositorFactory, RestfulCompositorFactory}
import com.didichuxing.horoscope.core.{Horoscope, Sources}
import com.didichuxing.horoscope.service.source.EventBuilders
import com.didichuxing.horoscope.util.Constants.SCH_SOURCE_FACTORY
import com.didichuxing.horoscope.util.{Logging, SystemLog, ThreadUtil}
import com.typesafe.config.{Config, ConfigFactory}

class LocalRunner extends Logging {

  def run(config: Config): Unit = {
    //初始化系统日志
    SystemLog.create()
    info("local horoscope init...")
    val apolloService = new ApolloService(config)
    apolloService.start()

    implicit def actorSystem = ActorSystem("HoroscopeSystem", config)

    implicit def actorMaterializer: ActorMaterializer = ActorMaterializer.create(actorSystem)

    implicit val restCompositorExecution = ThreadUtil.createThreadPool("restful-compositor", 256)
    implicit val restSchedulerContext = ThreadUtil.createScheduledThreadPool("restful-compositor", 20)
    //构造flowManager
    val flowManager = Horoscope.newLocalService()
      .withConfig(config)
      .withActorSystem(actorSystem)
      .withSourceFactory(SCH_SOURCE_FACTORY, Sources.scheduler(EventBuilders.schedulerSourceEventBuilder()))
      .withSourceFactory("batchJsonKafka", Sources.kafka(EventBuilders.sourceEventBuilder()))
      .withSourceFactory("batchJsonDDMQ", new DDMQSourceFactory(DDMQSourceBuilder.builder()))
      .withSourceFactory("jsonHttp", Sources.http())
      .build()
    //启动主服务
    info("horoscope begin start service")
    flowManager.startService()
    //启动外部source
    info("horoscope begin start sources")
    flowManager.startSources()
    info("horoscope init complete")
    //系统停止
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = {
        flowManager.stopAllSources()
        flowManager.stopService()
        apolloService.stop()
      }
    })
  }
}

object AppMain extends App {
  new LocalRunner().run(ConfigFactory.load("application.conf"))
}