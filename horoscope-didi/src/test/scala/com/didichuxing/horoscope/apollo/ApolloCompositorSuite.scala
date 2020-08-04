/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */
package com.didichuxing.horoscope.apollo

import com.didichuxing.horoscope.runtime._
import com.didichuxing.horoscope.runtime.convert.ValueTypeAdapter
import com.google.common.io.Resources
import com.google.gson.{GsonBuilder, JsonParser}
import com.typesafe.config.ConfigFactory
import com.xiaoju.apollo.sdk.Apollo
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._


class ApolloCompositorSuite extends FunSuite with Matchers with BeforeAndAfterAll with ScalaFutures {
  val jsonParser = new JsonParser
  val apolloStorePath = Resources.getResource("apollo_store").getPath
  implicit val gson = new GsonBuilder()
    .registerTypeHierarchyAdapter(classOf[Value], new ValueTypeAdapter)
    .create()

  override def beforeAll(): Unit = {
    Apollo.init(apolloStorePath)
  }


  override protected def afterAll(): Unit = {
    Apollo.close()
  }

  test("Read apollo toggle") {
    val config = ConfigFactory.parseString(
      """
        |apollo.toggle.name = "horoscope_toggle"
      """.stripMargin)
    val compositor = new ApolloToggleCompositor(config)
    val jsonArgs = jsonParser.parse(
      """
        |{
        |  "apollo_uid": "r#129202",
        |  "city_id": 1,
        |  "phone": "13999200290100"
        |}
      """.stripMargin)
    val args = Value(jsonArgs).asInstanceOf[ValueDict]
    val f = compositor.composite(args)
    val value = Await.result(f, 2 second)
    info(value.toJson)
    val dict = value.asInstanceOf[SimpleDict]
    dict.at(Constants.APOLLO_TOGGLE_ALLOW).get.as[BooleanValue].underlying should be (true)
  }

  test("Read apollo config") {
    val config = ConfigFactory.parseString(
      """
        |apollo.config.ns="horoscope_config"
        |apollo.config.name="horoscope_road_open_model"
      """.stripMargin)
    val compositor = new ApolloConfigCompositor(config)
    val args = new SimpleDict(Map.empty.iterator)
    val f = compositor.composite(args)
    val value = Await.result(f, 2 second)
    info(value.toJson)
    val dict = value.asInstanceOf[SimpleDict]
    dict.at("confidence").get.as[NumberValue].underlying.doubleValue() should be (0.5d +- 1e-6)
  }

}
