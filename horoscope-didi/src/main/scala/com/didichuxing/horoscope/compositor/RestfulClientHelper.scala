/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.compositor

import java.util.concurrent.ScheduledExecutorService

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest, HttpResponse, Uri}
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.didichuxing.horoscope.runtime.convert.ValueTypeAdapter
import com.didichuxing.horoscope.runtime.{Value, ValueDict}
import com.didichuxing.horoscope.util.AsyncUtil.retry
import com.didichuxing.horoscope.util.Logging
import com.google.gson.GsonBuilder

import scala.concurrent.{ExecutionContext, Future}

class RestfulClientHelper(
  implicit actorSystem: ActorSystem,
  actorMaterializer: ActorMaterializer,
  executor: ExecutionContext,
  scheduleExecutor: ScheduledExecutorService) extends Logging {
  implicit val gson = new GsonBuilder()
    .registerTypeHierarchyAdapter(classOf[Value], new ValueTypeAdapter)
    .create()

  val logViewLength: Int = 1024 // 在日志中显示的字符串长度
  def doPost(url: String)(postBody: Value): Future[Value] = {
    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = Uri(url),
      entity = HttpEntity(ContentTypes.`application/json`, postBody.toJson))
    retry[Value](10, 500) {
      Http().singleRequest(request).recover { case e: Exception => e }.flatMap {
        case resp@HttpResponse(code, _, entity, protocol) =>
          if (code.isSuccess()) {
            entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String).map { body =>
              info(s"Post restful request to url: $url with body: ${postBody.toJson.take(logViewLength)}, " +
                s"got response: ${body.take(logViewLength)}...")
              gson.fromJson(body, classOf[ValueDict])
            }
          } else {
            val errorStr = s"Post restful request to url: $url with body: ${postBody.toJson.take(logViewLength)} " +
              s"failed, with response code: ${code}"
            error(errorStr)
            resp.discardEntityBytes()
            Future.failed(CompositorException(errorStr))
          }
        case e: Throwable =>
          val errorStr = s"Post restful request to url: $url with body: ${postBody.toJson.take(logViewLength)} " +
            s"failed, with exception msg: ${e.getMessage}"
          error(errorStr)
          Future.failed(CompositorException(e.getMessage, Option(e)))
      }
    }
  }

  def doGet(url: String): Future[Value] = {
    val request = HttpRequest(HttpMethods.GET, uri = Uri(url))
    retry[Value](5, 500) {
      Http().singleRequest(request).recover { case e: Exception => e }.flatMap {
        case resp@HttpResponse(code, _, entity, protocol) =>
          if (code.isSuccess()) {
            entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String).map { body =>
              info(s"Get request to url: $url, got response: ${body.take(logViewLength)}...")
              gson.fromJson(body, classOf[Value])
            }
          } else {
            val errorStr = s"Get request to url: $url failed, with response code: ${code}"
            error(errorStr)
            resp.discardEntityBytes()
            Future.failed(CompositorException(errorStr))
          }
        case e: Throwable =>
          val errorStr = s"Get request to url:$url failed, with exception msg: ${e.getMessage}"
          error(errorStr)
          Future.failed(CompositorException(e.getMessage, Option(e)))
      }
    }
  }
}
