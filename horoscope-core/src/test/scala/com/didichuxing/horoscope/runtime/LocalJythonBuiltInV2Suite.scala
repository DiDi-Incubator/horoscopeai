package com.didichuxing.horoscope.runtime

import java.io.{File, IOException}
import java.nio.file.attribute.BasicFileAttributes

import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import com.didichuxing.horoscope.core.FileStore
import com.google.common.io.Resources
import java.nio.file.{FileVisitResult, FileVisitor, Files, Path, Paths}

import com.didichuxing.horoscope.runtime.expression.{BuiltIn, Expression, LocalJythonBuiltInV2}

import scala.collection.mutable.ListBuffer

class LocalJythonBuiltInV2Suite extends FunSuite with Matchers with MockFactory with BeforeAndAfterAll {
  import com.didichuxing.horoscope.util.FileUtils._
  private val fileStore = mock[FileStore]
  private val uri = Resources.getResource("").toURI
  private val basePath = Paths.get(uri).normalize()
  private val url = "infoflow"
  private var builtIn: BuiltIn = _


  override def beforeAll(): Unit = {
    (fileStore.listFiles _).expects(url).anyNumberOfTimes()
      .returning((basePath + "/" + url, walkFileTree(basePath.resolve(url), Set(".py"))))

    builtIn = new LocalJythonBuiltInV2(fileStore, url)
  }

  test("path2UDFName") {
    val (cwd, files) = fileStore.listFiles(url)
    cwd shouldBe basePath.normalize().toString + "/" + url
    files.map(_.getAbsolutePath) should contain (s"${basePath}/${url}/v2/entry/functions/add.py")
    files.map(_.getAbsolutePath) should contain(s"${basePath}/${url}/v2/functions/add.py")
    builtIn.path2UDFName(s"${basePath}/${url}/functions/add.py") shouldBe (("/", "add"))
    builtIn.path2UDFName(s"${basePath}/${url}/functions/test/add.py") shouldBe (("/test/", "add"))
    builtIn.path2UDFName(s"${basePath}/${url}/entry/flow1/functions/add.py") shouldBe (("/entry/flow1/", "add"))
    builtIn.path2UDFName(s"${basePath}/${url}/entry/flow1/methods/add.py") shouldBe (("/entry/flow1/", "add"))
  }

  test("evaluate") {
    Expression("/v2/entry/", "add(a, b)")(builtIn).evaluate(
      Value(Map("a" -> Value(1), "b" -> Value(1)))
    ).as[Int] shouldBe 2

    Expression("/v2/", "add(a, b)")(builtIn).evaluate(
      Value(Map("a" -> Value(1), "b" -> Value(1)))
    ).as[Int] shouldBe 3

    Expression("/v2/test", "add(a, b)")(builtIn).evaluate(
      Value(Map("a" -> Value(1), "b" -> Value(1)))
    ).as[Int] shouldBe 3
  }

}
