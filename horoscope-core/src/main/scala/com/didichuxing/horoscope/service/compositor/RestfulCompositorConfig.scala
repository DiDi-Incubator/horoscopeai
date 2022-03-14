package com.didichuxing.horoscope.service.compositor
import com.typesafe.config.Config

import scala.util.Try
import RestfulCompositorConfig._

class RestfulCompositorConfig(val config: Config, val flowConfig: RestfulConfigInFlow) {
  def getServiceUrl: String = flowConfig.url

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
  case class RestfulConfigInFlow(
    method: String, url: String, serviceName: Option[String],
    postRequestKey: Option[String] = None)

  val API_ADDRESS_IDENTITY = "@"
}
