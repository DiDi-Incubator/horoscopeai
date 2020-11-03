package com.didichuxing.horoscope.runtime

import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.didichuxing.horoscope.runtime.expression._
import com.didichuxing.horoscope.util.Logging
import org.scalatest.{FunSuite, Matchers}

class LocalJythonSuite extends FunSuite
  with Matchers
  with ScalatestRouteTest
  with Logging {

  implicit var builtIn: BuiltIn = _

  override def beforeAll(): Unit = {
    builtIn = newStore()
  }

  def newStore(): BuiltIn = {
    new LocalJythonBuiltIn().mergeFrom(DefaultBuiltIn.defaultBuiltin)
  }

  def eval(expression: String): Value = {
    Value(Map.empty[String, String]).asInstanceOf[ValueDict].eval(expression)
  }

  def eval(namespace: String, expression: String): Value = {
    Value(Map.empty[String, String]).asInstanceOf[ValueDict].eval(namespace, expression)
  }

  test("python udf") {
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
  }
}
