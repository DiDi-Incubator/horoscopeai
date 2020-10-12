/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.compositor

import java.util.concurrent.{LinkedBlockingQueue, ScheduledExecutorService, ThreadPoolExecutor, TimeUnit}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.didichuxing.horoscope.core.{Compositor, CompositorFactory}
import com.didichuxing.horoscope.runtime.convert.ValueTypeAdapter
import com.didichuxing.horoscope.runtime.{Value, ValueDict}
import com.didichuxing.horoscope.util.Logging
import com.didichuxing.horoscope.util.AsyncUtil._
import com.google.gson.GsonBuilder
import com.typesafe.config.Config

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
   * @param code: method[get/post] http://@{url}/${link}\n${post_body}
   *              @{url}: url will be from apollo config or local config
   *              ${variable}: variable will be filled at runtime
   * @return
   */
  override def create(code: String): RestfulCompositor = {
    val flowConfig = CompositorUtil.parseConfigFromFlow(code)
    val restfulConfig = new RestfulCompositorConfig(config, flowConfig)
    new RestfulCompositor(restfulConfig)
  }
}
