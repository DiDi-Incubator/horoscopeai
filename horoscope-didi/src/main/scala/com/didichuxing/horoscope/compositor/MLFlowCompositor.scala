/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.compositor

import java.util.concurrent.ScheduledExecutorService

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.didichuxing.horoscope.core.{Compositor, CompositorFactory}
import com.didichuxing.horoscope.runtime.convert.ValueTypeAdapter
import com.didichuxing.horoscope.runtime.{SimpleList, Value, ValueDict}
import com.didichuxing.horoscope.util.Logging
import com.didichuxing.horoscope.util.AsyncUtil.retry
import com.google.gson.GsonBuilder
import com.typesafe.config.Config

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

class MLFlowCompositor(config: RestfulCompositorConfig)(
  implicit actorSystem: ActorSystem,
  actorMaterializer: ActorMaterializer,
  executionContext: ExecutionContext,
  scheduleExecutor: ScheduledExecutorService
) extends Compositor with Logging {
  implicit val gson = new GsonBuilder()
    .registerTypeHierarchyAdapter(classOf[Value], new ValueTypeAdapter)
    .create()
  private val logViewLen: Int = 1024

  override def composite(args: ValueDict): Future[Value] = {
    try {
      val url = config.getServiceUrl
      val modelName = config.getModelName
      doPost(url, modelName)(args.visit(config.getPostBodyKey))
    } catch {
      case e: Throwable =>
        Future.failed(CompositorException(e.getMessage, Option(e)))
    }
  }

  def doPost(url: String, modelName: String)(postBody: Value): Future[Value] = {
    val request = getMLFlowRequest(postBody, url, modelName)
    retry[Value](5, 500) {
      Http().singleRequest(request).recover { case e: Exception => e }.flatMap {
        case resp@HttpResponse(code, _, entity, protocol) =>
          if (code.isSuccess()) {
            entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String).map { response =>
              info(s"Post mlflow request to model: $modelName with body: ${postBody.toJson.take(logViewLen)}, " +
                s"got response: $response")
              gson.fromJson(response, classOf[ValueDict])
            }
          } else {
            val errorStr = s"Post mlflow request to model: $modelName with body: ${postBody.toJson}" +
              s" failed, with response code: ${code}"
            error(errorStr)
            resp.discardEntityBytes()
            Future.failed(CompositorException(errorStr))
          }
        case e: Throwable =>
          val errorStr = s"Post mlflow request to model: $modelName with body: ${postBody.toJson} failed," +
            s" with exception: $e"
          error(errorStr)
          Future.failed(CompositorException(e.getMessage, Option(e)))
      }
    }
  }

  private def getMLFlowRequest(args: Value, url: String, modelName: String): HttpRequest = {
    val payload = new SimpleList(Seq(args)).toJson
    HttpRequest(
      method = HttpMethods.POST,
      uri = Uri(url),
      headers = immutable.Seq(RawHeader("didi-header-rid", modelName)),
      entity = HttpEntity(
        ContentType(MediaType.customWithFixedCharset(
          "application", "json; format=pandas-records", HttpCharsets.`UTF-8`)),
        payload
      )
    )
  }
}

class MLFlowCompositorFactory(config: Config)(
  implicit actorSystem: ActorSystem,
  actorMaterializer: ActorMaterializer,
  executionContext: ExecutionContext,
  scheduledExecutorService: ScheduledExecutorService
) extends CompositorFactory {

  override def name(): String = "mlflow"

  /**
   * @param code: method[get/post] http://@{url}/${link}\n${post_body}
   *              @{url}: url will be from apollo config or local config
   *              ${variable}: variable will be filled at runtime
   * @return
   */
  override def create(code: String): Compositor = {
    val flowConfig = CompositorUtil.parseConfigFromFlow(code)
    val restfulConfig = new RestfulCompositorConfig(config, flowConfig)
    new MLFlowCompositor(restfulConfig)
  }
}
