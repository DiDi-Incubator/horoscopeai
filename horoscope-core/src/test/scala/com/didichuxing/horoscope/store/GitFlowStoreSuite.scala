package com.didichuxing.horoscope.store

import java.io.File
import java.nio.file.Paths
import java.util.NoSuchElementException

import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.didichuxing.horoscope.core.{ConfigStore, FileStore}
import com.didichuxing.horoscope.runtime.LocalJythonBuiltInV2Suite
import com.didichuxing.horoscope.service.storage.GitFlowStore
import com.didichuxing.horoscope.util.FileUtils
import com.google.common.io.Resources
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.language.postfixOps

class GitFlowStoreSuite extends FunSuite
  with Matchers
  with MockFactory with ScalatestRouteTest with BeforeAndAfterAll {

  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import akka.http.scaladsl.model._
  import spray.json._
  import DefaultJsonProtocol._
  import com.didichuxing.horoscope.service.storage.ZookeeperConfigStore._

  private val configStore = mock[ConfigStore]
  private val uri = Resources.getResource("infoflow").toURI
  private val basePath = Paths.get(uri).normalize()
  private var flowStore: GitFlowStore = _

  class MockFileStore extends FileStore {
    override def api: Route = super.api

    override def listFiles(url: String): (String, Seq[File]) = {
      (basePath + "/", FileUtils.walkFileTree(basePath, Set(".py", ".flow", ".graphql")))
    }

    override def deleteFile(path: String): Boolean = ???

    override def getFile(path: String): File = ???

    override def updateFile(path: String, content: String): Boolean = ???

    override def createFile(path: String, isDirectory: Boolean): Boolean = ???

    override def copyFile(path: String): Boolean = ???

    override def renameFile(path: String, name: String): Boolean = ???
  }

  override def beforeAll(): Unit = {
    val fileStore = new MockFileStore()
    (configStore.getConfList _).expects(*).anyNumberOfTimes().returning(Nil)
    (configStore.registerListener _).expects(*).anyNumberOfTimes().returning(Unit)
    (configStore.registerChecker _).expects(*).anyNumberOfTimes().returning(Unit)

    flowStore = new GitFlowStore(configStore, fileStore, Map.empty, Map.empty)
  }

  test("flow store") {
    flowStore.getFlow("/v2/entry").flowDef.getName shouldBe "/v2/entry"
    flowStore.getController("/v2/entry") shouldBe Nil
  }

  test("resource store") {
    flowStore.getResource("/v2/entry/")("./demo.graphql").length shouldBe 132
    flowStore.getResource("/v2/")("entry/demo.graphql").length shouldBe 132
    flowStore.getResource("/v2/")("/v2/entry/demo.graphql").length shouldBe 132
    assertThrows[Exception] {
      flowStore.getResource("/v2/")("/entry/demo.graphql").length shouldBe 132
    }
  }

  test("flow store api") {
    (configStore.getConfList _).expects(*).anyNumberOfTimes().returning(Nil)
    (configStore.registerListener _).expects(*).anyNumberOfTimes().returning(Unit)
    (configStore.registerChecker _).expects(*).anyNumberOfTimes().returning(Unit)

    Get("/list") ~> flowStore.api ~> check {
      responseAs[Set[String]] should contain("/v2/entry")
      status shouldBe StatusCodes.OK
    }

    Get("/graph") ~> flowStore.api ~> check {
      status shouldBe StatusCodes.OK
    }

    Get("/controllers") ~> flowStore.api ~> check {
      status shouldBe StatusCodes.OK
      Seq[String]()
    }

    Post("/reload", "") ~> flowStore.api ~> check {
      status shouldBe StatusCodes.OK
    }
  }
}
