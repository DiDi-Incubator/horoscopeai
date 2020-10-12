/*
 *
 *  * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 *  * Authors: chaiyi@didichuxing.com
 *  * Description:
 *
 *
 */

package com.didichuxing.horoscope.builtin

import com.didichuxing.horoscope.runtime.{Value, ValueDict}
import com.didichuxing.horoscope.runtime.convert.ValueTypeAdapter
import com.didichuxing.horoscope.runtime.expression.DefaultBuiltIn
import com.google.gson.{Gson, GsonBuilder}
import org.scalatest.{FunSuite, Matchers}

class DateBuiltInSuite extends FunSuite with Matchers {
  implicit val gson: Gson = new GsonBuilder()
    .registerTypeHierarchyAdapter(classOf[Value], new ValueTypeAdapter)
    .create()

  implicit val builtIn = DateBuiltIn.builtin

  def eval(expression: String): Value = {
    Value(Map.empty[String, String]).asInstanceOf[ValueDict].eval(expression)
  }

  test("time align") {
    eval("""hour_ceil(1582810106)""").as[Long] shouldBe 1582812000
    eval("""hour_floor(1582810106)""").as[Long] shouldBe 1582808400
    eval("""day_ceil(1582810106)""").as[Long] shouldBe 1582819200
    eval("""day_floor(1582810106) """).as[Long] shouldBe 1582732800
    eval("""ten_minutes_ceil(1582810106)""").as[Long] shouldBe 1582810200
    eval("""ten_minutes_floor(1582810106)""").as[Long] shouldBe 1582809600

    eval("""hour_ceil(1582732800)""").as[Long] shouldBe 1582732800
    eval("""hour_floor(1582732800)""").as[Long] shouldBe 1582732800
    eval("""day_ceil(1582732800)""").as[Long] shouldBe 1582732800
    eval("""day_floor(1582732800) """).as[Long] shouldBe 1582732800
    eval("""ten_minutes_ceil(1582732800)""").as[Long] shouldBe 1582732800
    eval("""ten_minutes_floor(1582732800)""").as[Long] shouldBe 1582732800
  }

}
