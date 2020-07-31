/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.runtime

import scala.language.implicitConversions
import java.nio.charset.Charset

import com.didichuxing.horoscope.runtime
import com.didichuxing.horoscope.runtime.convert.{ValueCastException, ValueTypeAdapter}
import com.didichuxing.horoscope.util.Logging
import com.google.common.io.Resources
import com.google.gson.{Gson, GsonBuilder}
import org.scalatest.{FunSuite, Matchers}

class ExpressionSuite extends FunSuite with Matchers with Logging {
  import Implicits.builtin

  implicit val gson: Gson = new GsonBuilder()
    .registerTypeHierarchyAdapter(classOf[Value], new ValueTypeAdapter)
    .setPrettyPrinting()
    .create()

  // scalastyle:off
  def loadJson(name: String): ValueDict = {
    val resource = Resources.getResource(s"json/$name.json")
    gson.fromJson(Resources.toString(resource, Charset.defaultCharset()), classOf[ValueDict])
  }

  def json(text: String): Value = {
    gson.fromJson(text, classOf[Value])
  }

  val goessner: ValueDict = loadJson("goessner")
  val twitter: ValueDict = loadJson("twitter")
  val webapp: ValueDict = loadJson("webapp")

  def eval(expression: String): Value = {
    Value(Map.empty[String, String]).asInstanceOf[ValueDict].eval(expression)
  }

  test("literal") {
    eval("123").as[Int] shouldBe 123
    eval("-1").as[Long] shouldBe -1L
    eval("3.14").as[Double] shouldBe 3.14 +- 0.01
    eval(""" "hello world!" """).as[String] shouldBe "hello world!"
    eval("true").as[Boolean] shouldBe true
    eval("false").as[Boolean] shouldBe false
    eval("null") shouldBe NULL
  }

  test("new list") {
    eval("[]").as[Seq[Int]] shouldBe empty
    eval("[1, 2, 3]").as[Seq[Int]] shouldBe Seq(1, 2, 3)
  }

  test("new object") {
    eval(""" {"a": 1, "b": 2} """).as[Map[String, Int]] shouldBe Map(
      "a" -> 1,
      "b" -> 2
    )
  }

  test("visit") {
    goessner.eval("store.bicycle.color").as[String] shouldBe "red"

    goessner.eval("store.bicycle.*").as[Seq[Value]] shouldBe Seq(
      Value("red"),
      Value(19.95)
    )

    goessner.eval("store.book[*].author").as[Seq[String]] shouldBe Seq(
      "Nigel Rees",
      "Evelyn Waugh",
      "Herman Melville",
      "J. R. R. Tolkien"
    )

    twitter.eval("results.metadata.*").as[Seq[String]] should contain only "recent"
  }

  test("at") {
    goessner.eval(""" store.bicycle["color"] """).as[String] shouldBe "red"
    goessner.eval(""" store.book[0].title """).as[String] shouldBe "Sayings of the Century"
    goessner.eval(""" store.book[-1].title """).as[String] shouldBe "The Lord of the Rings"
    goessner.eval(""" store.book[*]["author"] """).as[Seq[String]] shouldBe Seq(
      "Nigel Rees",
      "Evelyn Waugh",
      "Herman Melville",
      "J. R. R. Tolkien"
    )
    goessner.eval(""" store.book[*]["isbn"] """).as[Seq[String]] shouldBe Seq(
      "0-553-21311-3", "0-395-19395-8"
    )
    goessner.eval(""" store[*][2].author """).as[Map[String, String]] shouldBe Map(
      "book" -> "Herman Melville"
    )
    goessner.eval(""" store[*][4].author """).as[Map[String, String]] shouldBe empty

    assertThrows[IndexOutOfBoundsException] {
      goessner.eval(""" store["car"] """)
    }
    assertThrows[IndexOutOfBoundsException] {
      goessner.eval(""" store.book[4] """)
    }
  }

  test("search") {
    goessner.eval("store..price").as[Seq[Double]] should have size 5
    goessner.eval("store..author").as[Seq[Value]] should have size 4
    goessner.eval("store..arthur") shouldBe NULL
    goessner.eval("store..*").as[Seq[Value]] should have size 25
  }

  test("select") {
    goessner.eval(""" store.book[0]["title", "author"] """).as[Map[String, String]] shouldBe Map(
      "title" -> "Sayings of the Century",
      "author" -> "Nigel Rees"
    )
    goessner.eval("store.book[-2, -1].title").as[Seq[String]] shouldBe Seq(
      "Moby Dick",
      "The Lord of the Rings"
    )
    goessner.eval("store.bicycle[*]").as[Map[String, Value]] shouldBe Map(
      "color" -> Value("red"),
      "price" -> Value(19.95)
    )
    goessner.eval(""" store.book[*]["title", "author"] """).as[Seq[Map[String, String]]] shouldBe Seq(
      Map("author" -> "Nigel Rees", "title" ->  "Sayings of the Century"),
      Map("author" -> "Evelyn Waugh", "title" ->  "Sword of Honour"),
      Map("author" -> "Herman Melville", "title" ->  "Moby Dick"),
      Map("author" -> "J. R. R. Tolkien", "title" ->  "The Lord of the Rings")
    )
    goessner.eval(""" store.book["a", "b"] """).as[Seq[Value]] shouldBe empty
  }

  test("slice") {
    goessner.eval("store.book[1:2].title").as[Seq[String]] shouldBe Seq(
      "Sword of Honour"
    )
    goessner.eval("store.book[:2].title").as[Seq[String]] shouldBe Seq(
      "Sayings of the Century",
      "Sword of Honour"
    )
    goessner.eval("store.book[-2:].title").as[Seq[String]] shouldBe Seq(
      "Moby Dick",
      "The Lord of the Rings"
    )
    goessner.eval("store.book[:].title").as[Seq[String]] should have size 4

    goessner.eval(""" store.book[0]["category" : "title"] """).as[Map[String, String]] shouldBe Map(
      "category" -> "reference",
      "author" -> "Nigel Rees",
      "title" -> "Sayings of the Century"
    )
    goessner.eval(
      """ store.book[0][:"author"] """
    ).as[Map[String, String]].keys.toSeq should contain theSameElementsAs Seq("category", "author")
    goessner.eval(
      """ store.book[0]["title":] """
    ).as[Map[String, Value]].keys.toSeq should contain theSameElementsAs Seq("title", "price")
  }

  test("query") {
    goessner.eval("store.book[?(_.isbn)].title").as[Seq[String]] shouldBe Seq(
      "Moby Dick", "The Lord of the Rings"
    )

    goessner.eval(
      """store.book[*][?(#[1] == "price" and _ <= expensive)]"""
    ).as[Seq[Map[String, Double]]] shouldBe Seq(
      Map("price" -> 8.95), Map("price" -> 8.99)
    )
  }

  test("builtin") {
    assertThrows[ValueCastException](
      eval("3.14").as[Int]
    )
    eval("round(3.14)").as[Int] shouldBe 3
    eval("round(3.5)").as[Int] shouldBe 4
    eval("ceil(3.14)").as[Int] shouldBe 4
    eval("floor(3.14)").as[Int] shouldBe 3
    eval("""substr("abc", 0, 1)""").as[String] shouldBe ("a")

    eval("[1, 2, 3].length()").as[Int] shouldBe 3
    eval(""" "hello world!".length() """).as[Int] shouldBe 12

    eval("[1, 4, -1, 3].max()").as[Int] shouldBe 4
    eval(""" ["n", "b", "a"].min() """).as[String] shouldBe "a"
    eval("[1.1, 2.2, 3.3].sum() ").as[Double] shouldBe 6.6 +- 0.1

    eval(""" ["M", "S"].subsetof(["M", "S", "L"]) """).as[Boolean] shouldBe true
    eval(""" ["M", "XL"].subsetof(["M", "S", "L"]) """).as[Boolean] shouldBe false
    eval(""" ["M", "XL"].anyof(["M", "S", "L"]) """).as[Boolean] shouldBe true
    eval(""" ["XS", "XL"].noneof(["M", "S", "L"]) """).as[Boolean] shouldBe true
  }

  test("predicate") {
    eval("1 == 1.0").as[Boolean] shouldBe true
    eval("1 != 1.0").as[Boolean] shouldBe false

    eval(""" "hello" == "hello" """).as[Boolean] shouldBe true
    eval(""" "hello" == "world" """).as[Boolean] shouldBe false

    eval("[1, 2] == [1, 2]").as[Boolean] shouldBe true
    eval("[1, 2] != [1, 2]").as[Boolean] shouldBe false
    eval(""" [1, ["a", "b"]] == [1, ["a", "b"]] """).as[Boolean] shouldBe true
    eval(""" [1] == [1, 2] """).as[Boolean] shouldBe false

    eval(" 1 < 1").as[Boolean] shouldBe false
    eval(" 1 < 2").as[Boolean] shouldBe true
    eval(" 1 <= 2").as[Boolean] shouldBe true
    eval(" 1 <= 1").as[Boolean] shouldBe true
    eval(" 1 > 1").as[Boolean] shouldBe false
    eval(" 2 > 1").as[Boolean] shouldBe true
    eval(" 1 > 2").as[Boolean] shouldBe false
    eval(" 1 >= 1").as[Boolean] shouldBe true

    eval(""" "a" < "b" """).as[Boolean] shouldBe true
    eval(""" "a" <= "b" """).as[Boolean] shouldBe true
    eval(""" "a" > "b" """).as[Boolean] shouldBe false

    eval(""" "b" in ["a", "b", "c"] """).as[Boolean] shouldBe true
    eval(""" "d" in ["a", "b", "c"] """).as[Boolean] shouldBe false
    eval(""" "a" in [] """).as[Boolean] shouldBe false

    eval(""" "a" in {"a": "b"} """).as[Boolean] shouldBe true
    eval(""" "b" in {"a": "b"} """).as[Boolean] shouldBe false
  }

  test("logical") {
    eval("not true").as[Boolean] shouldBe false
    eval("not false").as[Boolean] shouldBe true
    eval("not []").as[Boolean] shouldBe true
    eval("true and ([1, 2] == [1, 2, 3])").as[Boolean] shouldBe false
    eval("1 != 2 or 3 == 3").as[Boolean] shouldBe true
  }

  test("arithmetic") {
    eval("- (1 - 2)").as[Int] shouldBe 1
    eval("1 + 2").as[Int] shouldBe 3
    eval("1 - 2").as[Int] shouldBe -1
    eval("2 * 2").as[Int] shouldBe 4
    eval("1 / 2").as[Double] shouldBe 0.5
    assertThrows[ValueCastException] {
      eval("1 / 2").as[Int] shouldBe 0
    }
    eval("5 % 3").as[Int] shouldBe 2
    eval("5 % 2.4").as[Double] shouldBe 0.2
  }

  test("string ops") {
    eval(""" "a" + "b" """).as[String] shouldBe("ab")
    eval(""" "s" * 3 """).as[String] shouldBe("sss")
    eval(""" 2 + "b" """).as[String] shouldBe("2b")
  }

  test("list ops") {
    eval(""" [1] + [3]""").as[Seq[Int]] shouldBe Seq(1, 3)
    eval("""[1, 3] - [1] """).as[Seq[Int]] shouldBe Seq(3)
    eval(" null + [3] ").as[Seq[Int]] shouldBe Seq(3)
    eval(" [3] + null ").as[Seq[Int]] shouldBe Seq(3)

    goessner.eval("store.book.price + [store.bicycle.price]").as[Seq[Double]] should have size 5
  }

  test("transform") {
    goessner.eval("[ceil(_.price) <- store.book]").as[Seq[Int]] shouldBe Seq(
      9, 13, 9, 23
    )
  }

  test("group by") {
    goessner.eval("store.book[/(_.category)][*].title").as[Map[String, Seq[String]]] shouldBe Map(
      "reference" -> Seq("Sayings of the Century"),
      "fiction" -> Seq("Sword of Honour", "Moby Dick", "The Lord of the Rings")
    )
  }

  test("expand and join") {
    val result = goessner.eval(
      """store.book[*][*] + [expensive <- store.book[?(_.price < expensive)][+("price")]]"""
    )

    result.as[Seq[ValueDict]].head.visit("price").as[Int] shouldBe 10
  }

  test("demo") {
    goessner.eval("""store["bicycle"]""") shouldBe json(
      """{
        |  "color": "red",
        |  "price": 19.95
        |}""".stripMargin
    )

    goessner.eval("""store["book"][0]""") shouldBe json(
      """{
        |  "category": "reference",
        |  "author": "Nigel Rees",
        |  "title": "Sayings of the Century",
        |  "price": 8.95
        |}""".stripMargin
    )

    goessner.eval("""store["book"][-1]""") shouldBe json(
      """{
        | "category": "fiction",
        | "author": "J. R. R. Tolkien",
        | "title": "The Lord of the Rings",
        | "isbn": "0-395-19395-8",
        | "price": 22.99
        |}""".stripMargin
    )

    goessner.eval("""store.bicycle""") shouldBe json(
      """{
        |  "color": "red",
        |  "price": 19.95
        |}""".stripMargin
    )

    goessner.eval("""store.book.title""") shouldBe json(
      """[
        |  "Sayings of the Century",
        |  "Sword of Honour",
        |  "Moby Dick",
        |  "The Lord of the Rings"
        |]""".stripMargin
    )

    goessner.eval("""store.*""") shouldBe json(
      """[
        |  {
        |    "category": "reference",
        |    "author": "Nigel Rees",
        |    "title": "Sayings of the Century",
        |    "price": 8.95
        |  },
        |  {
        |    "category": "fiction",
        |    "author": "Evelyn Waugh",
        |    "title": "Sword of Honour",
        |    "price": 12.99
        |  },
        |  {
        |    "category": "fiction",
        |    "author": "Herman Melville",
        |    "title": "Moby Dick",
        |    "isbn": "0-553-21311-3",
        |    "price": 8.99
        |  },
        |  {
        |    "category": "fiction",
        |    "author": "J. R. R. Tolkien",
        |    "title": "The Lord of the Rings",
        |    "isbn": "0-395-19395-8",
        |    "price": 22.99
        |  },
        |  {
        |    "color": "red",
        |    "price": 19.95
        |  }
        |]""".stripMargin
    )

    goessner.eval("""store.*.price""") shouldBe json(
      """[8.95, 12.99, 8.99, 22.99, 19.95]"""
    )

    goessner.eval("""store..price""") shouldBe json(
      """[8.95, 12.99, 8.99, 22.99, 19.95]"""
    )

    goessner.eval("""store.game""") shouldBe json("""null""")

    goessner.eval("""expensive.price""") shouldBe json("""null""")

    goessner.eval("""store.game.price""") shouldBe json("""null""")

    goessner.eval("""store.book.publisher""") shouldBe json("""null""")

    goessner.eval("""store.*.discount""") shouldBe json("""null""")

    goessner.eval("""store[*]""") shouldBe goessner.eval("""store[*][*]""")

    goessner.eval("""store.*.discount""") shouldBe json("""null""")

    goessner.eval("""store.book[-2:]["category", "price"]""") shouldBe json(
      """[
        |  {
        |    "category": "fiction",
        |    "price": 8.99
        |  },
        |  {
        |    "category": "fiction",
        |    "price": 22.99
        |  }
        |]""".stripMargin
    )

    goessner.eval("""store.book[1, 3]["title" : "price"]""") shouldBe json(
      """[
        |  {
        |    "price": 12.99,
        |    "title": "Sword of Honour"
        |  },
        |  {
        |    "isbn": "0-395-19395-8",
        |    "price": 22.99,
        |    "title": "The Lord of the Rings"
        |  }
        |]""".stripMargin
    )

    goessner.eval("""store.book[?(_.price < expensive)]["author", "title"]""") shouldBe json(
      """[
        |  {
        |    "author": "Nigel Rees",
        |    "title": "Sayings of the Century"
        |  },
        |  {
        |    "author": "Herman Melville",
        |    "title": "Moby Dick"
        |  }
        |]""".stripMargin
    )

    goessner.eval("""store.book[/(_.category)]""") shouldBe json(
      """{
        |  "fiction": [
        |    {
        |      "category": "fiction",
        |      "author": "Evelyn Waugh",
        |      "title": "Sword of Honour",
        |      "price": 12.99
        |    },
        |    {
        |      "category": "fiction",
        |      "author": "Herman Melville",
        |      "title": "Moby Dick",
        |      "isbn": "0-553-21311-3",
        |      "price": 8.99
        |    },
        |    {
        |      "category": "fiction",
        |      "author": "J. R. R. Tolkien",
        |      "title": "The Lord of the Rings",
        |      "isbn": "0-395-19395-8",
        |      "price": 22.99
        |    }
        |  ],
        |  "reference": [
        |    {
        |      "category": "reference",
        |      "author": "Nigel Rees",
        |      "title": "Sayings of the Century",
        |      "price": 8.95
        |    }
        |  ]
        |}""".stripMargin
    )

    goessner.eval("""store.book[/(_.category)][*].title""") shouldBe json(
      """{
        |  "fiction": [
        |    "Sword of Honour",
        |    "Moby Dick",
        |    "The Lord of the Rings"
        |  ],
        |  "reference": [
        |    "Sayings of the Century"
        |  ]
        |}""".stripMargin
    )

    goessner.eval(
    """store.book[*][, "title"] + [_.price >= expensive <- store.book[*]][+ "is_expensive"]"""
    ) shouldBe json(
      """[
        |  {
        |    "is_expensive": false,
        |    "title": "Sayings of the Century"
        |  },
        |  {
        |    "is_expensive": true,
        |    "title": "Sword of Honour"
        |  },
        |  {
        |    "is_expensive": false,
        |    "title": "Moby Dick"
        |  },
        |  {
        |    "is_expensive": true,
        |    "title": "The Lord of the Rings"
        |  }
        |]""".stripMargin
    )

    goessner.eval("""store.book[*]["title", "author"]'""") shouldBe json(
      """{
        |  "author": [
        |    "Nigel Rees",
        |    "Evelyn Waugh",
        |    "Herman Melville",
        |    "J. R. R. Tolkien"
        |  ],
        |  "title": [
        |    "Sayings of the Century",
        |    "Sword of Honour",
        |    "Moby Dick",
        |    "The Lord of the Rings"
        |  ]
        |}""".stripMargin
    )
  }
}
