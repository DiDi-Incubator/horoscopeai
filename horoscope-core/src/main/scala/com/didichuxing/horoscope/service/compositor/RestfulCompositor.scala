package com.didichuxing.horoscope.service.compositor

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.didichuxing.horoscope.core.{Compositor, CompositorFactory}
import com.didichuxing.horoscope.runtime.{Value, ValueDict}
import com.didichuxing.horoscope.util.Logging
import com.typesafe.config.Config

import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.{ExecutionContext, Future}

class RestfulCompositor(
  restfulConfig: RestfulCompositorConfig
)(implicit actorSystem: ActorSystem,
  actorMaterializer: ActorMaterializer,
  executionContext: ExecutionContext,
  scheduleExecutor: ScheduledExecutorService
) extends RestfulClientHelper(restfulConfig.config) with Compositor with Logging {

  override def composite(args: ValueDict): Future[Value] = {
    try {
      val url = restfulConfig.getServiceUrl
      restfulConfig.getHttpMethod match {
        case "post" =>
          doPost(CompositorUtil.updateUrlByArguments(url, args))(args.visit(restfulConfig.getPostBodyKey))
        case "get" =>
          doGet(CompositorUtil.updateUrlByArguments(url, args))
        case m@_ =>
          Future.failed(CompositorException(s"Unsupported restful method: $m"))
      }
    } catch {
      case e: Throwable =>
        Future.failed(CompositorException(e.getMessage, Option(e)))
    }
  }
}

class RestfulCompositorFactory(config: Config)(
  implicit actorSystem: ActorSystem,
  actorMaterializer: ActorMaterializer,
  executionContext: ExecutionContext,
  scheduleExecutor: ScheduledExecutorService
) extends CompositorFactory {

  override def name(): String = "restful"

  /**
   * @param code: method[get/post] http://url/${link}\n${post_body}
   *              ${variable}: variable will be filled at runtime
   * @return
   */
  override def create(code: String)(resource: String => Array[Byte]): RestfulCompositor = {
    val flowConfig = CompositorUtil.parseConfigFromFlow(code)
    val restfulConfig = new RestfulCompositorConfig(config, flowConfig)
    new RestfulCompositor(restfulConfig)
  }
}
