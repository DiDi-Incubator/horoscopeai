/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.compositor

import com.didichuxing.horoscope.runtime.{Value, ValueDict}
import com.didichuxing.horoscope.compositor.RestfulCompositorConfig._
import com.typesafe.config.{Config, ConfigFactory}
import com.xiaoju.apollo.sdk.Apollo
import com.didichuxing.horoscope.compositor.convert.Implicits._

import scala.util.Try

class RestfulCompositorConfig(config: Config, flowConfig: RestfulConfigInFlow) {
  //private val compositorConfig = config.getConfig("traffic.intelligence-gateway-service.restful-compositor")
  private val namespace = Try(config.getString("apollo.namespace")).getOrElse("")
  private val configName = Try(config.getString("apollo.config-name")).getOrElse("")
  private val defaultConfig = Try(config.getConfig("default")).getOrElse(ConfigFactory.empty())

  def getServiceUrl: String = {
    if (!flowConfig.url.contains(API_ADDRESS_IDENTITY)) {
      // use url in flow
      flowConfig.url
    } else {
      // use url in apollo config
      try {
        val apolloConfig = Apollo.getConfig(namespace, configName)
        val valueDict = Value(apolloConfig).asInstanceOf[ValueDict]
        CompositorUtil.updateUrlByConfig(flowConfig.url, valueDict)
      } catch {
        case e: Throwable =>
          // get apollo config error, fallback to local config
          CompositorUtil.updateUrlByConfig(flowConfig.url, Value(defaultConfig).asInstanceOf[ValueDict])
      }
    }
  }

  def getHttpMethod: String = {
    flowConfig.method
  }

  def getPostBodyKey: String = {
    val size = flowConfig.postRequestKey.get.length
    Try (flowConfig.postRequestKey.get.substring(2, size - 1)).getOrElse("")
  }

  def getModelName: String = {
    flowConfig.serviceName.get
  }
}

object RestfulCompositorConfig {

  // serviceName: MLFlow modelName
  case class RestfulConfigInFlow(
    method: String, url: String, serviceName: Option[String],
    postRequestKey: Option[String] = None)

  val API_ADDRESS_IDENTITY = "@"
}
