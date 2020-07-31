/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: chaiyi@didiglobal.com
 */

package com.didichuxing.horoscope.runtime
import com.didichuxing.horoscope.runtime.convert.ValueTypeAdapter
import com.didichuxing.horoscope.runtime.expression.DefaultBuiltIn
import com.google.gson.{Gson, GsonBuilder}
import org.scalatest.{FunSuite, Matchers}

import scala.language.implicitConversions

class DefaultBuiltInSuite extends FunSuite with Matchers {
  implicit val gson: Gson = new GsonBuilder()
    .registerTypeHierarchyAdapter(classOf[Value], new ValueTypeAdapter)
    .create()

  implicit val builtIn = DefaultBuiltIn.defaultBuiltin

  def eval(expression: String): Value = {
    Value(Map.empty[String, String]).asInstanceOf[ValueDict].eval(expression)
  }

  test("zip two dict list") {
    eval("""[{"l1":1}, {"l2": 2}].zip([{"r1": 1}, {"r2": 2}])""").as[Seq[Map[String, Int]]] shouldBe
      Seq(Map("l1" -> 1, "r1" -> 1), Map("l2" -> 2, "r2" -> 2))

    // duplicate key
    eval("""[{"l1":1}, {"l2": 2}].zip([{"l1": 1}, {"r2": 2}])""").as[Seq[Map[String, Int]]] shouldBe
      Seq(Map("l1" -> 1), Map("l2" -> 2, "r2" -> 2))

    // filter zip result
    eval("""[{"l1":1}, {"l2": 2}].zip([{"l1": 1}, {"r2": 2}])[?(_.l1 < 2)]""").as[Seq[Map[String, Int]]] shouldBe
      Seq(Map("l1" -> 1))
  }

  test("sort seq[dict] by key") {
    eval("""[{"key": 10}, {"key": 1}, {"key": 20}].sort_by_key("key")""").as[Seq[Map[String, Int]]] shouldBe
      Seq(Map("key" -> 1), Map("key" -> 10), Map("key" -> 20))
  }

  test("trans") {
    val dict = new SimpleDict(Map("a" -> Value(10), "b" -> Value("11"), "c" -> Value("1.0")))
    dict.eval("""a.to_string()""").as[String] shouldBe "10"
    dict.eval("""b.to_long()""").as[Long] shouldBe 11
    dict.eval("""c.to_double()""").as[Double] shouldBe 1.0 +- 1e-6
  }

  test("flatten") {
    eval("""[[1,2],[3,4]].flatten()""").as[Seq[Int]] shouldBe Seq(1, 2, 3, 4)
  }

  // scalastyle:off
  test("parse json") {
    gson.fromJson("""{"a": {"1980091":{"confusable_struct_link_min_width":["{\"timestamp\": 1581933600, \"featureBuffer\": [{\"mapVersion\": 2020021718, \"value\": 19.064529006410208}]}"]}}}
                    |""".stripMargin, classOf[ValueDict]
    ).eval("""parse_json(a["1980091"].confusable_struct_link_min_width[0]).featureBuffer[0].value""").as[Double] shouldBe
      19.06 +- 0.01
  }

  test("string split") {
    eval(""" split("10,100", ",") """).as[Array[String]] shouldBe Array("10", "100")
  }

  test("mk string") {
    eval("""mk_string(["a", "b", "c"], ",")""").as[String] shouldBe "a,b,c"
  }

  test("distinct") {
    eval("""[1, 2, 3, 1].distinct()""").as[Array[Int]] shouldBe Array(1, 2, 3)
  }

  test("to_json") {
    eval("""{"k1":1, "k2":2}.to_json()""").as[String] shouldBe
      new SimpleDict(Map("k1" -> Value(1), "k2" -> Value(2))).toJson
  }

}
