package com.didichuxing.horoscope.runtime

import java.nio.file.{Files, Path, Paths}
import java.util.UUID

import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.didichuxing.horoscope.runtime.expression.{ZookeeperJythonBuiltIn, _}
import com.didichuxing.horoscope.util.Logging
import com.google.common.io.Resources
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryForever
import org.apache.curator.test.TestingServer
import org.scalatest.{FunSuite, Matchers}

class ZookeeperJythonSuite extends FunSuite
  with Matchers
  with ScalatestRouteTest
  with Logging {

  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import akka.http.scaladsl.model._
  import spray.json._
  import DefaultJsonProtocol._

  implicit var builtIn: BuiltIn = _

  val zookeeper: TestingServer = new TestingServer(false)

  val curator: CuratorFramework = CuratorFrameworkFactory.builder()
    .connectString(zookeeper.getConnectString)
    .namespace("")
    .retryPolicy(new RetryForever(1000))
    .build()

  val funcDir: Path = Paths.get(Resources.getResource("python").toURI)

  def funcContent(path: String): String = new String(Files.readAllBytes(funcDir.resolve(path + ".py")))

  override def beforeAll(): Unit = {
    zookeeper.start()
    curator.start()
    builtIn = newStore()
  }

  override def afterAll(): Unit = {
    curator.close()
    zookeeper.stop()
  }

  def newStore(): BuiltIn = {
    new ZookeeperJythonBuiltIn(curator.usingNamespace(UUID.randomUUID().toString))
      .mergeFrom(DefaultBuiltIn.defaultBuiltin)
  }

  def eval(expression: String): Value = {
    Value(Map.empty[String, String]).asInstanceOf[ValueDict].eval(expression)
  }

  def eval(namespace: String, expression: String): Value = {
    Value(Map.empty[String, String]).asInstanceOf[ValueDict].eval(namespace, expression)
  }

  test("put get udf") {
    Put("/tree/functions/add", funcContent("functions/add")) ~> builtIn.api ~> check {
      status shouldBe StatusCodes.OK
    }
    Put("/tree/functions/test/add", funcContent("functions/test/add")) ~> builtIn.api ~> check {
      status shouldBe StatusCodes.OK
    }
    Put("/tree/functions/tuple", funcContent("functions/tuple")) ~> builtIn.api ~> check {
      status shouldBe StatusCodes.OK
    }
    Put("/tree/methods/hello", funcContent("methods/hello")) ~> builtIn.api ~> check {
      status shouldBe StatusCodes.OK
    }
    Put("/tree/functions/test/nd4j/nd4j_add", funcContent("functions/test/nd4j/nd4j_add")) ~> builtIn.api ~> check {
      status shouldBe StatusCodes.OK
    }
    // wait work
    Thread.sleep(1000)
    eval(""""horoscope".length()""").as[Int] shouldBe 9
    eval("/flow1", "add(1, 2)").as[Int] shouldBe 3
    eval("/test/flow2", "add(1, 2)").as[Int] shouldBe 4
    eval("""tuple(["hello", "world"])""").as[Seq[String]] shouldBe Seq(
      "world",
      "hello"
    )
    eval("/test-flow", """"horoscope".hello()""").as[String] shouldBe "hello horoscope!"
    eval("/test/nd4j/demo-flow", "nd4j_add(1, 2)").as[Seq[Seq[Double]]] shouldBe Seq(
      Seq(3.0, 3.0),
      Seq(3.0, 3.0),
      Seq(3.0, 3.0)
    )

    Get("/tree/") ~> builtIn.api ~> check {
      responseAs[JsValue].convertTo[Set[String]] shouldBe Set(
        "/functions/", "/methods/"
      )
    }

    Get("/tree/functions/") ~> builtIn.api ~> check {
      responseAs[JsValue].convertTo[Set[String]] shouldBe Set(
        "/functions/add", "/functions/test/", "/functions/tuple"
      )
    }

    Get("/tree/methods/") ~> builtIn.api ~> check {
      responseAs[JsValue].convertTo[Set[String]] shouldBe Set(
        "/methods/hello"
      )
    }
  }

}
