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
import com.didichuxing.horoscope.runtime.{IgnoredException, SimpleList, Value, ValueDict}
import com.didichuxing.horoscope.util.Logging
import com.didichuxing.horoscope.util.AsyncUtil.retry
import com.google.gson.GsonBuilder
import com.typesafe.config.Config

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.collection.JavaConverters._

class MLFlowCompositor(restfulConfig: RestfulCompositorConfig)(
  implicit actorSystem: ActorSystem,
  actorMaterializer: ActorMaterializer,
  executionContext: ExecutionContext,
  scheduleExecutor: ScheduledExecutorService
) extends Compositor with Logging {
  implicit val gson = new GsonBuilder()
    .registerTypeHierarchyAdapter(classOf[Value], new ValueTypeAdapter)
    .create()

  private val config: Config = restfulConfig.config
  private val retryNum: Int = Try(config.getInt("client.retry-attempts")).getOrElse(0)
  private val retryInterval: Int = Try(config.getInt("client.retry-interval")).getOrElse(500)
  private val nonRetriedErrCode: Set[Int] = Try(
    config.getIntList("client.ignored-error-code").asScala.map(_.toInt).toSet
  ).getOrElse(Set())

  // 在日志中显示的字符串长度
  private val logViewLength: Int = 1024

  override def composite(args: ValueDict): Future[Value] = {
    try {
      val url = restfulConfig.getServiceUrl
      val modelName = restfulConfig.getModelName
      doPost(url, modelName)(args.visit(restfulConfig.getPostBodyKey))
    } catch {
      case e: Throwable =>
        Future.failed(CompositorException(e.getMessage, Option(e)))
    }
  }

  def doPost(url: String, modelName: String)(postBody: Value): Future[Value] = {
    val request = getMLFlowRequest(postBody, url, modelName)
    retry[Value](retryNum, retryInterval) {
      Http().singleRequest(request).recover { case e: Exception => e }.flatMap {
        case resp@HttpResponse(code, _, entity, protocol) =>
          if (code.isSuccess()) {
            entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String).map { response =>
              info(s"Post mlflow request to model: $modelName with body: ${postBody.toJson.take(logViewLength)}, " +
                s"got response: $response")
              gson.fromJson(response, classOf[ValueDict])
            }
          } else {
            val errorStr = s"Post mlflow request to model: $modelName with body: ${postBody.toJson}" +
              s" failed, with response code: ${code}"
            error(errorStr)
            resp.discardEntityBytes()

            if (nonRetriedErrCode.contains(code.intValue())) {
              Future.failed(IgnoredException(errorStr))
            } else {
              Future.failed(CompositorException(errorStr))
            }
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
