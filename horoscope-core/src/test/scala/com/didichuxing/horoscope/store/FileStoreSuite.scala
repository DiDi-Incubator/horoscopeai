package com.didichuxing.horoscope.store

import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.didichuxing.horoscope.service.storage.LocalFileStore
import com.didichuxing.horoscope.util.Logging
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import java.io.File
import java.nio.file.Files
import scala.collection.mutable.ListBuffer
import scala.sys.process._

class FileStoreSuite extends FunSuite
  with BeforeAndAfterAll
  with ScalatestRouteTest
  with Logging {
  val config: Config = ConfigFactory.parseString(
    s"""
       |horoscope.storage.file-store.local-root-path = "./horoscope-core/src/test/resources/flow"
       |horoscope.storage.file-store.type = ["flow","graphQL","py"]
     """.stripMargin)
  val store = new LocalFileStore(config)
  val outputResult: ListBuffer[String] = ListBuffer[String]()
  val errorResult: ListBuffer[String] = ListBuffer[String]()
  val commandLogger: ProcessLogger = ProcessLogger(line => outputResult.append(line),
    line => errorResult.append(line))

  override def beforeAll(): Unit ={
    store.createDirectories(store.rootPath)
  }

  override def afterAll(): Unit = {
    store.deleteFile(store.rootPath)
  }

  test("create three empty directories") {
    val dirPath1 = store.rootPath + "/traffic/helloWorld"
    val dirPath2 = store.rootPath + "/traffic/dev"
    val dirPath3 = store.rootPath + "/traffic/dev/analysis"
    assert(store.createFile(dirPath1, isDirectory = true))
    assert(store.createFile(dirPath2, isDirectory = true))
    assert(store.createFile(dirPath3, isDirectory = true))
    assert(new File(dirPath1).isDirectory)
    assert(new File(dirPath3).isDirectory)
    assert(new File(dirPath3).isDirectory)
  }

  test("create empty Files") {
    val filePath1 = store.rootPath + "/traffic/helloWorld/hello.flow"
    val filePath2 = store.rootPath + "/traffic/dev/analysis/user-report.flow"
    val filePath3 = store.rootPath + "/traffic/dev/analysis/info.graphQL"
    assert(store.createFile(filePath1, isDirectory = false))
    assert(store.createFile(filePath2, isDirectory = false))
    assert(store.createFile(filePath3, isDirectory = false))
    assert(new File(filePath1).isFile)
    assert(new File(filePath2).isFile)
    assert(new File(filePath3).isFile)
  }

  test("list all files") {
    debug(("Files List", store.listFiles("traffic").toString()))
  }

  test("get Directory tree") {
    val p = (store.rootPath + "/traffic").split("/")
    debug(("directory", p.toList.toString))
    debug(("tree", store.walkFile(p.toList)))
  }

  test("update files") {
    val content = "# /helloWorld/hello\n\n\n***\n    a <- {\"in_city_list\": true}\n    b <- {\"b\": 1} + a      " +
      "\n    \n    \n\n\n***"
    val filePath = store.rootPath + "/traffic/helloWorld/hello.flow"
    assert(store.updateFile(filePath, content))
    val source = new File(filePath)
    debug(("content:", Files.readAllLines(source.toPath).toArray.mkString("", "\n", "")))
  }

  test("copy files") {
    val filePath = store.rootPath + "/traffic/helloWorld/hello.flow"
    assert(store.copyFile(filePath))
    debug(("after copy:", store.listFiles("traffic")))
  }

  test("rename files") {
    val filePath = store.rootPath + "/traffic/helloWorld/hello_copy.flow"
    assert(store.renameFile(filePath, "hallo.flow"))
    debug(("after rename:", store.listFiles("traffic")))
  }

  test("delete files") {
    val filePath = store.rootPath + "/traffic/helloWorld/hallo.flow"
    assert(store.deleteFile(filePath))
    info(("after delete:", store.listFiles("traffic")))
  }

}
