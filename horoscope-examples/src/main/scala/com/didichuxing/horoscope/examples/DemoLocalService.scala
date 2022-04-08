package com.didichuxing.horoscope.examples

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.didichuxing.horoscope.core.{Horoscope, Sources}
import com.didichuxing.horoscope.examples.builtins.ExampleBuiltIn
import com.didichuxing.horoscope.service.compositor.RestfulCompositorFactory
import com.didichuxing.horoscope.service.local.FlowManager
import com.didichuxing.horoscope.service.storage.{GitFlowStore, LocalConfigStore, LocalFileStore}
import com.didichuxing.horoscope.util.{Logging, SystemLog}
import com.typesafe.config.{Config, ConfigFactory}
import com.didichuxing.horoscope.util.ThreadUtil


class DemoLocalService extends Logging {

  var flowManager: FlowManager = _
  val config: Config = ConfigFactory.load(Thread.currentThread().getContextClassLoader, "application")

  implicit def actorSystem: ActorSystem = ActorSystem("demo-local", config)
  implicit def actorMaterializer: ActorMaterializer = ActorMaterializer.create(actorSystem)
  implicit val restCompositorExecution = ThreadUtil.createThreadPool("restful-compositor", 256)
  implicit val restSchedulerContext = ThreadUtil.createScheduledThreadPool("restful-compositor", 20)

  def run(): Unit = {
    SystemLog.create()
    info("local service init")
    val flowStore = GitFlowStore.newBuilder()
      .withFileStore(new LocalFileStore(config))
      .withConfigStore(new LocalConfigStore)
      .withCompositorFactory("restful", new RestfulCompositorFactory(config))
      .withBuiltIn(ExampleBuiltIn.exampleBuiltin)
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
