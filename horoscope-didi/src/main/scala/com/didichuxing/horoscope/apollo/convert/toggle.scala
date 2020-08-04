/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */
package com.didichuxing.horoscope.apollo.convert

import com.didichuxing.horoscope.runtime.{SimpleDict, Value}
import com.didichuxing.horoscope.apollo.ApolloException
import com.xiaoju.apollo.sdk.FeatureToggle

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
  * From ApolloFeatureToggle -> Value
  */
class FeatureToggleConverter extends Value.From[FeatureToggle] with AnyRefConvertible {
  type ValueType = Value
  import com.didichuxing.horoscope.apollo.Constants._

  override def apply(toggle: FeatureToggle): Value = {
    val featureDict = new mutable.HashMap[String, Value]()
    // 0 for SUCCESS, -1 for failure, -2 for toggle not exists
    toggle.getRetCode match {
      case 0 =>
        featureDict += APOLLO_TOGGLE_NAME -> Value(toggle.getToggleName)
        featureDict += APOLLO_UID -> Value(toggle.getUserId)
        featureDict += APOLLO_TOGGLE_ALLOW -> Value(toggle.allow())
        val experiment = toggle.getExperiment
        if (experiment != null) {
          featureDict += APOLLO_GROUP_NAME -> Value(experiment.getGroupName)
          // flatten parameters
          val allParams = experiment.getAllParameters
          allParams.asScala.foreach { case (k, v) =>
              featureDict += k -> Value(v)
          }
        }
        new SimpleDict(featureDict.toMap)

      case -1 =>
        throw new ApolloException(s"Apollo toggle code -1, " +
          s"which means get toggle ${toggle.getToggleName} failed with reason ${toggle.getRetMsg}")

      case -2 =>
        throw new ApolloException(s"Apollo toggle code -2, " +
          s"which means toggle ${toggle.getToggleName} not exists")
    }
  }
}

trait FeatureToggleConvertible {
  implicit val fromFeatureToggle: Value.From.Aux[FeatureToggle, Value] = new FeatureToggleConverter
}
