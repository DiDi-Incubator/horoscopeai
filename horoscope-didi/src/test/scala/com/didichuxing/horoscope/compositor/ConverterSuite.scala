/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.compositor

import com.didichuxing.horoscope.runtime.{Value, _}
import com.didichuxing.horoscope.compositor.convert.Implicits._
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

import scala.language.implicitConversions

class ConverterSuite extends FunSuite with Matchers {
  test("config converter") {
    val config = ConfigFactory.parseString(
      """
        |  traffic {
        |    intelligence-gateway-service {
        |      restful-compositor {
        |        apollo-namespace = "horoscope_compositor"
        |        apollo-config-name = "test_config"
        |      }
        |    }
        |  }
        |""".stripMargin)

    val value = Value(config)
    assert(value.isInstanceOf[ValueDict])
    val dict = value.asInstanceOf[ValueDict]
    info(dict.toString)
  }
}
