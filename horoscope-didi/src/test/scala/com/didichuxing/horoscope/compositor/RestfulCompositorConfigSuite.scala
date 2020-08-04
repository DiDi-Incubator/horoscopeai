/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.compositor

import com.google.common.io.Resources
import com.typesafe.config.ConfigFactory
import com.xiaoju.apollo.sdk.Apollo
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class RestfulCompositorConfigSuite extends FunSuite with Matchers with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    try {
      val apolloStore = Resources.getResource("apollo_store").getPath
      Apollo.init(apolloStore)
    } catch {
      case e: Throwable =>
        info(e.getMessage)
    }
  }

  override def afterAll(): Unit = {
    Apollo.close()
  }

  val localConfig = ConfigFactory.parseString(
    """
      | apollo {
      |   namespace = "horoscope_config"
      |   config-name = "restful_compositor"
      | }
      | default = {
      |   road-open-api = "http://10.1.1.1/road-open"
      | }
      |""".stripMargin)

  test("load flow config") {
    val code = "get http://10.1.1.1/road-close/${link}"
    info(CompositorUtil.parseConfigFromFlow(code).toString)
    val compositorConfig = new RestfulCompositorConfig(localConfig, CompositorUtil.parseConfigFromFlow(code))

    compositorConfig.getServiceUrl shouldBe "http://10.1.1.1/road-close/${link}"
    compositorConfig.getHttpMethod shouldBe "get"
  }

  test("load local config in conf file") {
    val code = "get @{road-open-api}/${link}"
    info(CompositorUtil.parseConfigFromFlow(code).toString)
    val compositorConfig = new RestfulCompositorConfig(localConfig, CompositorUtil.parseConfigFromFlow(code))
    compositorConfig.getServiceUrl shouldBe "http://10.1.1.1/road-open/${link}"
    compositorConfig.getHttpMethod shouldBe "get"
  }

  test("load apollo config ") {
    val code = "get @{road-close-api}/${link}"
    val flowConfig = CompositorUtil.parseConfigFromFlow(code)

    val compositorConfig = new RestfulCompositorConfig(localConfig, flowConfig)
    compositorConfig.getServiceUrl shouldBe "http://10.1.1.1/road-close/${link}"
    compositorConfig.getHttpMethod shouldBe "get"
  }
}
