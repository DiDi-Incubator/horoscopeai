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

class GraphQLCompositorSuite extends FunSuite with Matchers with BeforeAndAfterAll {

  val config = ConfigFactory.load("application.conf")
  private var bindFuture: Future[ServerBinding] = _
  implicit val actorSystem: ActorSystem = ActorSystem("rest-compositor-suite")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher
  implicit val schedulerExecutor = ThreadUtil.createScheduledThreadPool("rest-compositor-schedule", 4)
  implicit val gson = new GsonBuilder()
    .registerTypeHierarchyAdapter(classOf[Value], new ValueTypeAdapter)
    .create()

  private val mockServer = "http://localhost:18089/road-close-v3"
  val uri = URI.create(mockServer)
  val mockResult = new SimpleDict(Map("confidence" -> Value(0.9), "description" -> Value("null"))).toJson

  override def beforeAll(): Unit = {
    bindFuture = {
      Http().bindAndHandleAsync(
        (request: HttpRequest) => {
          if (request.uri.path.startsWith(Path(uri.getPath))) {
            Future(HttpResponse(StatusCodes.OK, Nil, HttpEntity(mockResult)))
          } else {
            Future(HttpResponse(404))
          }
        }, uri.getHost, uri.getPort)
    }
  }

  test("do post") {
    val code = "post http://localhost:18089/road-close-v3 demo.graphql"
    val valueDict = new SimpleDict(Map("user_id" -> Value(100001)))
    try {
      info(new GraphQLCompositorFactory(config).create(code).composite(valueDict).await().toString)
    } catch {
      case e: Throwable =>
        info(e.getMessage)
    }
  }

  override def afterAll(): Unit = {
    bindFuture.flatMap(_.unbind()).onComplete(_ => actorSystem.terminate())
  }
}
