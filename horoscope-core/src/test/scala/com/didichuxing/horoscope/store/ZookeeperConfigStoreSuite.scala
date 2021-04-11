package com.didichuxing.horoscope.store

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.didichuxing.horoscope.core.ConfigChangeListener
import com.didichuxing.horoscope.service.storage.{ZookeeperConfigStore, ZookeeperFlowStore}
import com.typesafe.config.Config
import org.apache.curator.framework.recipes.cache.TreeCache
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryForever
import org.apache.curator.test.TestingServer
import org.apache.zookeeper.CreateMode
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers, stats}
import spray.json.JsValue

import java.util.UUID
import java.util.concurrent.Executors
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class ZookeeperConfigStoreSuite extends FunSuite
  with Matchers
  with MockFactory
  with BeforeAndAfter
  with ScalatestRouteTest {

  val zookeeper: TestingServer = new TestingServer(false)
  val curator: CuratorFramework = CuratorFrameworkFactory.builder()
    .connectString(zookeeper.getConnectString)
    .retryPolicy(new RetryForever(1000))
    .build()

  def newStore(): ZookeeperConfigStore = {
    new ZookeeperConfigStore(curator.usingNamespace("config"))
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
    an[IllegalArgumentException] should be thrownBy store.getLogConf("nothing")
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
    val config = store.getLogConf("test")
    config shouldBe a[Config]
  }

  test("getLofConfList return List[Config]") {
    putByCurator()
    // wait for put
    Thread.sleep(1000)

    val store = newStore()
    val list = store.getLogConfList
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

    store.register(listener)
    putByCurator()
    Thread.sleep(1000)
    listener.messages should contain("config has changed === I will update my config")
  }
}



