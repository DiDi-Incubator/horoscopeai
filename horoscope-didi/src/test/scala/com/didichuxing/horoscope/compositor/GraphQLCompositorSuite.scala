/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.compositor

import java.io.File
import java.net.URI

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model.{HttpEntity, HttpRequest, HttpResponse, StatusCodes}
import akka.stream.ActorMaterializer
import com.didichuxing.horoscope.runtime.convert.ValueTypeAdapter
import com.didichuxing.horoscope.runtime.{NULL, SimpleDict, SimpleList, Text, Value}
import com.didichuxing.horoscope.util.AsyncUtil.FutureAwait
import com.didichuxing.horoscope.util.ThreadUtil
import com.google.common.io.{Files, Resources}
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

  private val rootPath = Resources.getResource("graphql").getFile
  private val root = new File(rootPath)
  private val fileList = root.listFiles().filter(_.isFile)
  private val graphQLList: Map[String, Array[Byte]] = fileList
    .map(file => {
      val filename = file.getName
      val content = Files.toByteArray(new File(file.getAbsolutePath))
      filename -> content
    }).toMap

  def getResource(name: String): Array[Byte] = {
    graphQLList(name)
  }

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

  test("do post demo.graphql") {
    val code =
      """post http://10.85.128.137/map/warehouse/graphql?apikey=93d27872c74a43a5b014d7c4108ee0ae demo.graphql
        |header cache.key=order_id;cache.ttl=1
        |query ($order_id: Long!) {
        |    order(orderId: $order_id, orderType: GS) {
        |        orderBase {
        |            orderId
        |            driverId
        |        }
        |    }
        |}
        |""".stripMargin
    val valueDict = Value(Map[String, Long]("order_id" -> 17695669003487L))
    try {
      info(new GraphQLCompositorFactory(config).create(code)(getResource).composite(valueDict).await().toString)
    } catch {
      case e: Throwable =>
        info(e.getMessage)
    }
  }

  test("do post driver-user-order.graphql") {
    val code = "post http://10.85.128.137/map/warehouse/graphql?apikey=93d27872c74a43a5b014d7c4108ee0ae " +
      "driver-user-order.graphql"
    val valueDict = new SimpleDict(Map("driver_id" -> Value(580542422972833L), "start"-> Value(1596023886),
      "end" -> Value(1596094334)))
    try {
      info(new GraphQLCompositorFactory(config).create(code)(getResource).composite(valueDict).await().toString)
    } catch {
      case e: Throwable =>
        info(e.getMessage)
    }
  }

  override def afterAll(): Unit = {
    bindFuture.flatMap(_.unbind()).onComplete(_ => actorSystem.terminate())
  }
}
