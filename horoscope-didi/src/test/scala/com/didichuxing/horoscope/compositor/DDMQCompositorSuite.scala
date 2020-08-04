/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.compositor

import com.didichuxing.horoscope.runtime.{SimpleDict, Value}
import com.didichuxing.horoscope.runtime.convert.ValueTypeAdapter
import com.didichuxing.horoscope.service.source.DefaultSourceExecutionContext
import com.google.gson.GsonBuilder
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.concurrent.duration._
import scala.concurrent.Await

class DDMQCompositorSuite extends FunSuite with Matchers with BeforeAndAfterAll {

  implicit val gson = new GsonBuilder()
    .registerTypeHierarchyAdapter(classOf[Value], new ValueTypeAdapter)
    .create()
  val sec = DefaultSourceExecutionContext(ConfigFactory.empty())
  implicit val ec = sec.getExecutionContext()

  ignore("ddmq compositor suite") {
    val code =
      """
        | topic="horoscope_source"
        | env="test"
        | public-log-key="map_traffic_trajectory_overspeed"
        |""".stripMargin
    val list = Value(List("1", "2", "3"))
    val dict = Value(Map("a"->Value("1"), "b"->Value("2"), "c"->Value(false)))
    val args = Value(Map("a"->Value("1"), "b"->Value(2.323214124),
      "e"->Value(200000000), "c"->dict, "d"->list, "f"->Value(true)))
    val body = Value(Map("body"->dict))
    val compositor = new DDMQCompositorFactory().create(code)
    for(_ <- 0 until 1) {
      val f = compositor.composite(args)
      val value = Await.result(f, 5 second)
      info(value.toJson)
    }
  }

}
