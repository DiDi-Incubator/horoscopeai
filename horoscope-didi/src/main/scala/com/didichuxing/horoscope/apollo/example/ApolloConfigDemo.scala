/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */
package com.didichuxing.horoscope.apollo.example

import com.xiaoju.apollo.sdk.Apollo
import scala.collection.JavaConverters._

// scalastyle:off
object ApolloConfigDemo {
  def main(args: Array[String]): Unit = {
    Apollo.autoInit()
    val namespace = args(0)
    val configName = args(1)
    val config = Apollo.getConfig(namespace, configName)
    println(config.stringValues().asScala.mkString(", "))
    Apollo.close()
  }
}
