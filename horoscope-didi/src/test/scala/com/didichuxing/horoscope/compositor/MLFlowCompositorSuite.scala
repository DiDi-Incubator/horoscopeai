/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */
package com.didichuxing.horoscope.compositor

import java.net.URI

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model.{HttpEntity, HttpRequest, HttpResponse, StatusCodes}
import akka.stream.ActorMaterializer
import com.didichuxing.horoscope.runtime.convert.ValueTypeAdapter
import com.didichuxing.horoscope.runtime.{SimpleDict, Value}
import com.didichuxing.horoscope.util.AsyncUtil.FutureAwait
import com.didichuxing.horoscope.util.ThreadUtil
import com.google.gson.GsonBuilder
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.concurrent.{ExecutionContextExecutor, Future}

class MLFlowCompositorSuite extends FunSuite with Matchers with BeforeAndAfterAll {

  val config = ConfigFactory.load("application.conf")
  val compositorConfig = config.getConfig("traffic.intelligence-gateway-service.restful-compositor")
  private var bindFuture: Future[ServerBinding] = _
  implicit val actorSystem: ActorSystem = ActorSystem("mlflow-compositor-suite")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher
  implicit val schedulerExecutor = ThreadUtil.createScheduledThreadPool("mlflow-compositor-schedule", 4)
  implicit val gson = new GsonBuilder()
    .registerTypeHierarchyAdapter(classOf[Value], new ValueTypeAdapter)
    .create()

  private val mockServer = "http://localhost:18088/road-open"
  val uri = URI.create(mockServer)
  val mockResult = new SimpleDict(Map("confidence" -> Value(0.9), "description" -> Value("null"))).toJson

  override def beforeAll(): Unit = {
    bindFuture = {
      Http().bindAndHandleAsync(
        (request: HttpRequest) => {
          if (request.uri.path.startsWith(Path(uri.getPath))) {
            info(request.entity.toString)
            Future(HttpResponse(StatusCodes.OK, Nil, HttpEntity(mockResult)))
          } else {
            Future(HttpResponse(404))
          }
        }, uri.getHost, uri.getPort)
    }
  }

  test("do post") {
    val code = "post http://localhost:18088/road-open road-open\n${link}"
    val valueDict = new SimpleDict(Map("link" -> Value(100001)))
    try {
      info(new MLFlowCompositorFactory(compositorConfig).create(code).composite(valueDict).await().toString)
    } catch {
      case e: Throwable =>
        info(e.getMessage)
    }
  }

  ignore("post hourly_close_model_v3_pyfunc in pre env") {
    val code = "post http://100.69.238.11:8000/map/warehouse/ms hourly_close_model_v3_pyfunc\n${args}"
    val args = new SimpleDict(Map("entityId" -> Value("r#90000317065041"), "eventTime" -> Value(1583806926000L),
      "featureTime" -> Value(1583810400000L)))
    try {
      info(new MLFlowCompositorFactory(compositorConfig).create(code).composite(args).await().toString)
    } catch {
      case e: Throwable =>
        info(e.getMessage)
    }
  }

  override def afterAll(): Unit = {
    bindFuture.flatMap(_.unbind()).onComplete(_ => actorSystem.terminate())
  }
}
