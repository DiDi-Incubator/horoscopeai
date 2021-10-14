package com.didichuxing.horoscope.store

import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.didichuxing.horoscope.core.ConfigChangeListener
import com.didichuxing.horoscope.service.storage.{ZookeeperConfigStore, ZookeeperFlowStore}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryForever
import org.apache.curator.test.TestingServer
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.data.Stat
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers, stats}

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable
import scala.util.parsing.json.JSONArray

class ZookeeperConfigStoreSuite extends FunSuite
  with Matchers
  with MockFactory
  with BeforeAndAfter
  with ScalatestRouteTest {
  import ZookeeperConfigStore._
  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import akka.http.scaladsl.model._
  import spray.json._
  import DefaultJsonProtocol._

  val zookeeper: TestingServer = new TestingServer(false)
  val curator: CuratorFramework = CuratorFrameworkFactory.builder()
    .connectString(zookeeper.getConnectString)
    .retryPolicy(new RetryForever(1000))
    .build()

  def newStore(): ZookeeperConfigStore = {
    val store = new ZookeeperConfigStore(curator.usingNamespace("config"), null)
    store
  }

  override def beforeAll(): Unit = {
    zookeeper.start()
    curator.start()
  }

  override def afterAll(): Unit = {
    curator.close()
    zookeeper.stop()
  }

  def putByCurator():Unit = {
    val path = "/config/log/test"
    val text = """{"name" : "test"}"""
    val stat = curator.checkExists().forPath(path)
    if (stat == null) {
      curator.create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.PERSISTENT)
        .forPath(path, text.getBytes())
    } else {
      curator.setData()
        .forPath(path, text.getBytes())
    }
  }

  test("try to get by method - empty") {
    val store = newStore()
    an[IllegalArgumentException] should be thrownBy store.getConf("nothing", LOG_TYPE)
  }

  test("get config list through api") {
    val store = newStore()

    Get("/configs/subscription") ~> store.api ~> check {
      responseAs[String] shouldBe """{"subscriptions" : []}"""
    }
  }

  test("get config list by type") {
    val store = newStore()
    val list: List[Config] = store.getConfList(LOG_TYPE)
    list shouldBe a[List[Any]]
  }

  // tests of operating zk directly
  test("put and get by curator") {
    putByCurator()
    // wait for put
    Thread.sleep(1000)

    // get
    new String(curator.getData.forPath("/config/log/test")) should === ("""{"name" : "test"}""")
  }

  test ("list of children") {
    putByCurator()
    // wait for put
    Thread.sleep(1000)


    val configTypePath = "/config/log"
    val list = curator.getChildren().forPath(configTypePath).asScala
    list shouldBe a[mutable.Buffer[String]]
  }

  test("getLogConf return Config") {
    putByCurator()
    // wait for put
    Thread.sleep(1000)

    val store = newStore()
    val config = store.getConf("test", LOG_TYPE)
    config shouldBe a[Config]
  }

  test("getLofConfList return List[Config]") {
    putByCurator()
    // wait for put
    Thread.sleep(1000)

    val store = newStore()
    val list = store.getConfList(LOG_TYPE)
    list shouldBe a[List[Config]]
  }

  test("Config Listeners") {
    trait Output {
      def print(s: String):Unit = Console.println(s)
    }

    trait MockOutput extends Output {
      var messages: Seq[String] = Seq()
      override def print(s: String):Unit = messages = messages :+ s
    }

    class ListenerTest extends ConfigChangeListener with Output {
      override def onConfUpdate():Unit = print("config has changed === I will update my config")
    }
    val listener = new ListenerTest with MockOutput

    val store = newStore()

    store.registerListener(listener)
    putByCurator()
    Thread.sleep(1000)
    listener.messages should contain("config has changed === I will update my config")
  }

  test("version conf api") {
    val store = newStore()
    val data =
      """
        |{
        |  "name": "test",
        |  "data": [],
        |  "version": {"modify_user": "test", "modify_time": ""}
        |}
        |""".stripMargin

    Put("/config/log/test", HttpEntity(ContentTypes.`application/json`, data)) ~> store.api ~> check {
      status shouldBe StatusCodes.OK
    }

    Get("/config/log/test") ~> store.api ~> check {
      ConfigFactory.parseString(entityAs[String]).getString("version.id") shouldBe "1"
      info(entityAs[String])
    }

    Put("/config/log/test", HttpEntity(ContentTypes.`application/json`, data)) ~> store.api ~> check {
      status shouldBe StatusCodes.OK
    }

    Get("/config/log/test") ~> store.api ~> check {
      ConfigFactory.parseString(entityAs[String]).getString("version.id") shouldBe "2"
      info(entityAs[String])
    }

    val rollbackData =
      """
        |{
        | "rollback_version": 1,
        | "modify_user": "nobody",
        | "modify_time": "now"
        |}
        |""".stripMargin

    Post("/config/log/test/rollback", HttpEntity(ContentTypes.`application/json`, rollbackData)) ~> store.api~> check {
      status shouldBe StatusCodes.OK
    }

    Get("/config/log/test") ~> store.api ~> check {
      ConfigFactory.parseString(entityAs[String]).getString("version.id") shouldBe "3"
      info(s"conf test after rollback: ${entityAs[String]}")
    }

    Get("/config/log/test/versions") ~> store.api ~> check {
      status shouldBe StatusCodes.OK
      info(s"versions, ${entityAs[String]}")
    }

    Get("/config/log/test/3") ~> store.api ~> check {
      info(s"version 3, ${entityAs[String]}")
    }

    Get("/configs/log") ~> store.api ~> check {
      status shouldBe StatusCodes.OK
      info(s"config list, ${entityAs[String]}")
    }

    Thread.sleep(1000)
  }
}



