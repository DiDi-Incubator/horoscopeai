/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */
package com.didichuxing.horoscope.apollo

import java.util

import com.didichuxing.horoscope.runtime._
import com.google.gson.JsonParser
import com.xiaoju.apollo.sdk.model.api.{Config, DefaultFeatureToggle}
import com.xiaoju.apollo.sdk.{ApolloUser, Experiment, FeatureToggle}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.collection.JavaConverters._
import scala.language.implicitConversions

class ConverterSuite extends FunSuite with Matchers with BeforeAndAfterAll with MockFactory {

  val jsonParser = new JsonParser

  import com.didichuxing.horoscope.apollo.convert.implicits._

  test("ValueDict to ApolloUser") {
    val jsonArgs = jsonParser.parse(
      """
        |{
        |  "apollo_uid": "r#129202",
        |  "city_id": null,
        |  "phone": "13999200290100"
        |}
      """.stripMargin)
    val args = Value(jsonArgs)
    val apolloUser = args.as[ApolloUser]
    apolloUser.getId should be ("r#129202")
    apolloUser.getMap.asScala.get("city_id").isEmpty should be (true)
    apolloUser.getMap.asScala.get("phone").get should be ("13999200290100")
  }

  test("FeatureToggle to Value") {
    val toggleName = "horoscope_toggle"
    val uid = "r#1000"
    val groupName = "treatment_group"
    val params: util.Map[String, AnyRef] = Map(
      "k1" -> 1,
      "k2" -> 1.3f,
      "k3" -> false,
      "k4" -> 2L,
      "k5" -> 0.5d,
      "k6" -> "a",
      "k7" -> Map("k71" -> 1, "k72" -> "2", "k73" -> List(1, 2, 3), "k74" -> Map("k711" -> null)),
      "k8" -> List(1, 2, 3)
    ).asJava.asInstanceOf[java.util.Map[java.lang.String, AnyRef]]
    val expr = mock[Experiment]
    (expr.getGroupName _).expects().anyNumberOfTimes().returning(groupName)
    (expr.getAllParameters _).expects().anyNumberOfTimes().returning(params)

    val ft: FeatureToggle = new DefaultFeatureToggle(toggleName, uid) {
      override def allow(): Boolean = true
      override def getExperiment: Experiment = expr
      override def getRetCode: Int = 0
    }

    val value = Value(ft)
    info(value.toString)

    val dict = value.asInstanceOf[SimpleDict]
    dict.at(Constants.APOLLO_UID).get.asInstanceOf[Text].underlying should be (uid)
    dict.at(Constants.APOLLO_TOGGLE_NAME).get.as[Text].underlying should be (toggleName)
    dict.at(Constants.APOLLO_TOGGLE_ALLOW).get.as[BooleanValue].underlying should be (true)
    dict.at("k1").get.asInstanceOf[NumberValue].underlying.intValue() should be (1)
    dict.at("k2").get.asInstanceOf[NumberValue].underlying.doubleValue() should be (1.3d +- 1e-3)
  }

  test("Apollo config to Value") {
    val namespace = "horoscope"
    val configName = "road_close"
    val values = Map("p1" -> 1, "p2" -> "abc", "switch" -> false)
      .asJava.asInstanceOf[java.util.Map[java.lang.String, AnyRef]]
    val version = "v1"
    val config = new Config(namespace, configName, values, version)
    val value = Value(config)
    info(value.toString)

    val dict = value.asInstanceOf[SimpleDict]
    dict.at(Constants.APOLLO_NAMESPACE).get.asInstanceOf[Text].underlying should be (namespace)
    dict.at(Constants.APOLLO_CONFIG_NAME).get.as[Text].underlying should be (configName)
    dict.at(Constants.APOLLO_CONFIG_VERSION).get.as[Text].underlying should be (version)
    dict.at("switch").get.asInstanceOf[BooleanValue].underlying should be (false)
  }

}
