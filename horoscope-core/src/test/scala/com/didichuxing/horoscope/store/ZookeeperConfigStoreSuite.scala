package com.didichuxing.horoscope.store

import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.didichuxing.horoscope.service.storage.ZookeeperConfigStore
import com.typesafe.config.Config
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryForever
import org.apache.curator.test.TestingServer
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.data.Stat
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable

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
    new ZookeeperConfigStore(curator.usingNamespace("config"), "/hnb_test/config")
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

  test("get config list with no folder exist") {
    val store = newStore()
    if(curator.checkExists().forPath("/config/log") != null) {
      curator.delete().forPath("/config/log")
    }
    val stat: Stat = curator.checkExists().forPath("/config/log")
    (stat == null) shouldBe(true)

    val s = store.getChildrenContent("log")
    s shouldBe("""{"log-conf" : []}""")
  }

  test("get config list through api") {
    val store = newStore()

    Get("/configs/subscription") ~> store.api ~> check {
      responseAs[String] shouldBe """{"subscriptions" : []}"""
    }
  }

  test("get config list by type") {
    val store = newStore()
    val list: List[Config] = store.getLogConfList
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
}
