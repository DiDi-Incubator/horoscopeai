/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.store

import java.nio.file.{Files, Path, Paths}
import java.util.UUID

import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.didichuxing.horoscope.core.FlowDslMessage.FlowDef
import com.didichuxing.horoscope.dsl.FlowCompiler
import com.didichuxing.horoscope.service.storage.ZookeeperFlowStore
import com.google.common.io.Resources
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryForever
import org.apache.curator.test.TestingServer
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.io.StdIn

class ZookeeperFlowStoreSuite extends FunSuite
  with Matchers
  with MockFactory
  with BeforeAndAfterAll
  with ScalatestRouteTest {
  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import akka.http.scaladsl.model._
  import spray.json._
  import DefaultJsonProtocol._

  val zookeeper: TestingServer = new TestingServer(false)

  val curator: CuratorFramework = CuratorFrameworkFactory.builder()
    .connectString(zookeeper.getConnectString)
    .namespace("")
    .retryPolicy(new RetryForever(1000))
    .build()

  val flowDir: Path = Paths.get(Resources.getResource("infoflow").toURI)

  def newStore(): ZookeeperFlowStore = {
    new ZookeeperFlowStore(curator.usingNamespace(UUID.randomUUID().toString))
  }

  def flowText(path: String): String = new String(Files.readAllBytes(flowDir.resolve(path + ".flow")))

  def flowDef(path: String): FlowDef = FlowCompiler.compile(flowText(path))

  override def beforeAll(): Unit = {
    zookeeper.start()
    curator.start()
  }

  override def afterAll(): Unit = {
    curator.close()
    zookeeper.stop()
  }

  test("empty") {
    val store = newStore()
    an[NoSuchElementException] should be thrownBy store.getFlowByName("/something")
  }

  test("put and get") {
    val store = newStore()

    Put("/tree/test/batch", flowText("batch")) ~> store.api ~> check {
      status shouldBe StatusCodes.OK
    }
    // wait delete to work
    Thread.sleep(1000)

    Get("/tree/test/batch") ~> store.api ~> check {
      responseAs[String] shouldBe flowText("batch")
    }

    Get("/tree/") ~> store.api ~> check {
      responseAs[JsValue].convertTo[Set[String]] shouldBe Set(
        "/test/"
      )
    }

    Get("/tree/test/") ~> store.api ~> check {
      responseAs[JsValue].convertTo[Set[String]] shouldBe Set(
        "/test/batch"
      )
    }

    store.getFlowByName("/test/batch").getId == flowDef("batch").getId
  }

  test("batch load and delete") {
    val store = newStore()
    store.load(flowDir)

    // wait delete to work
    Thread.sleep(1000)

    Get("/tree/") ~> store.api ~> check {
      responseAs[JsValue].convertTo[Set[String]] shouldBe Set(
        "/test/",
        "/v2/",
        "/traffic/",
        "/root/"
      )
    }

    Delete("/tree/v2") ~> store.api ~> check {
      status shouldBe StatusCodes.OK
    }

    // wait delete to work
    Thread.sleep(1000)

    Get("/tree/") ~> store.api ~> check {
      responseAs[JsValue].convertTo[Set[String]] shouldBe Set(
        "/test/",
        "/traffic/",
        "/root/"
      )
    }
  }

  ignore("local-http-server") {
    import akka.http.scaladsl.server.Directives._

    val store = newStore()
    store.load(flowDir)

    val route: Route = {
      pathPrefix("api") {
        pathPrefix("flow") {
          store.api
        }
      }
    }

    Http().bindAndHandle(route, "localhost", 1234)
    StdIn.readLine()
  }
}
