/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */
package com.didichuxing.horoscope.apollo

import com.didichuxing.horoscope.core.{Compositor, CompositorFactory}
import com.didichuxing.horoscope.runtime.{Value, ValueDict}
import com.typesafe.config.{Config, ConfigFactory}
import com.xiaoju.apollo.sdk.Apollo
import com.xiaoju.apollo.sdk.model.api

import scala.concurrent.Future


class ApolloConfigCompositorFactory extends CompositorFactory {
  override def name(): String = "apollo_config"

  override def create(code: String): Compositor = {
    try {
      val config = ConfigFactory.parseString(code)
      new ApolloConfigCompositor(config)
    } catch {
      case e: Exception =>
        throw new ApolloException(s"Invalid apollo toggle config ${code}", Some(e.getCause))
    }
  }
}

/**
  * 1. Example
  * * RoadOpenModelConfig
  * ``` apollo_config
  * apollo.config.ns="horoscope_config"
  * apollo.config.name="hourly_open_flow_parameter"
  * ```
  * config <- RoadOpenModelConfig()
  *
  * return result format is:
  * {
  * "apollo_namespace": "horoscope_config",
  * "apollo_config_version": "1582250202000",
  * "apollo_config_name": "horoscope_road_open_model"
  * "yaml": "name: foodmart\npath:\n  lib\ncache: true\nmaterializations:\n- [ Materialization... ]",
  * "bool_value": true,
  * "confidence": 0.5,
  * "string_value": "abc",
  * "json_value": {
  *    "k2": "v2",
  *    "confidence": 0.8,
  *    "k1": "v1"
  *  }
  * }
  *
  * 2. Call [[com.xiaoju.apollo.sdk.Apollo.autoInit]] when application startup
  *
  * @param config typesafe config format
  */
class ApolloConfigCompositor(config: Config) extends Compositor {
  import com.didichuxing.horoscope.apollo.convert.implicits._
  val namespace = config.getString("apollo.config.ns")
  val configName = config.getString("apollo.config.name")

  override def composite(args: ValueDict): Future[Value] = {
    try {
      val config: api.Config = Apollo.getConfig(namespace, configName)
      val result = Value(config)
      Future.successful(result)
    } catch {
      case e: Throwable =>
        Future.failed(e)
    }
  }
}
