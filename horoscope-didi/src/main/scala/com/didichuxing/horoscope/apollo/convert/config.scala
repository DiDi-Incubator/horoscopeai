/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */
package com.didichuxing.horoscope.apollo.convert

import java.util

import com.didichuxing.horoscope.runtime.{SimpleDict, Value}
import com.didichuxing.horoscope.apollo.ApolloException
import com.xiaoju.apollo.sdk.model.api.Config

import scala.collection.JavaConverters._
import scala.collection.mutable


trait ApolloConfigConvertible {
  implicit val fromApolloConfig: Value.From.Aux[Config, Value] = new ApolloConfigConverter
}

class ApolloConfigConverter extends Value.From[Config] with AnyRefConvertible {
  import com.didichuxing.horoscope.apollo.Constants._
  type ValueType = Value

  def apply(ac: Config): Value = {
    if (!ac.valid()) {
      throw new ApolloException(s"Invalid apollo config, namespace ${ac.getNamespace}, " +
        s"config name ${ac.getConfigName}, error msg ${ac.getErrorMessage}")
    } else {
      val confDict = new mutable.HashMap[String, Value]()
      confDict += APOLLO_NAMESPACE -> Value(ac.getNamespace)
      confDict += APOLLO_CONFIG_NAME -> Value(ac.getConfigName)
      confDict += APOLLO_CONFIG_VERSION -> Value(ac.getVersion)
      val rawValues: util.Map[String, java.lang.Object] = ac.rawValues()
      rawValues.asScala.foreach { case (k, v) =>
        confDict += k -> Value(v)
      }
      new SimpleDict(confDict.toMap)
    }
  }
}
