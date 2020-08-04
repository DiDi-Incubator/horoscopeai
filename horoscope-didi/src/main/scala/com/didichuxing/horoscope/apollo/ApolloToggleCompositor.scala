/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */
package com.didichuxing.horoscope.apollo

import com.didichuxing.horoscope.core.{Compositor, CompositorFactory}
import com.didichuxing.horoscope.runtime.{Value, ValueDict}
import com.typesafe.config.{Config, ConfigFactory}
import com.xiaoju.apollo.sdk.{Apollo, ApolloUser}

import scala.concurrent.Future


class ApolloToggleCompositorFactory extends CompositorFactory {
  override def name(): String = "apollo_toggle"

  override def create(code: String): Compositor = {
    try {
      val config = ConfigFactory.parseString(code)
      new ApolloToggleCompositor(config)
    } catch {
      case e: Exception =>
        throw new ApolloException(s"Invalid apollo toggle config ${code}", Some(e.getCause))
    }
  }
}

/**
  * 1.Example
  * * RoadOpenToggle
  * ``` apollo_toggle
  * apollo.toggle.name = "horoscope_toggle"
  * ```
  * when use
  * toggle_result <- RoadOpenToggle(apollo_uid=key, city=xxx, ...)
  *
  * return result if hit toggle:
  * {
  * "apollo_group_name":"control_group",
  * "apollo_toggle_allow":true,
  * "apollo_uid":"r#129202",
  * "apollo_toggle_name":"horoscope_toggle",
  * "param1":1,
  * "param2":"control"
  * }
  *
  *
  * 2.Call [[com.xiaoju.apollo.sdk.Apollo.autoInit]] when application startup
  *
  * @param config typesafe config format
  */
class ApolloToggleCompositor(config: Config) extends Compositor {
  import com.didichuxing.horoscope.apollo.convert.implicits._
  val toggleName = config.getString("apollo.toggle.name")

  override def composite(args: ValueDict): Future[Value] = {
    try {
      val apolloUser = args.as[ApolloUser]
      val featureToggle = Apollo.getToggleByName(toggleName, apolloUser)
      val result = Value(featureToggle)
      Future.successful(result)
    } catch {
      case e: Throwable =>
        Future.failed(e)
    }
  }
}
