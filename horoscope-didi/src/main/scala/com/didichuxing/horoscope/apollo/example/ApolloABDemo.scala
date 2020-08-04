/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */
package com.didichuxing.horoscope.apollo.example

import com.xiaoju.apollo.sdk.{Apollo, ApolloUser}
import scala.collection.JavaConverters._

object ApolloABDemo {

  // scalastyle:off
  def main(args: Array[String]): Unit = {
    Apollo.autoInit()
    val user = new ApolloUser("173739290").`with`("level", "1")
    val toggle = Apollo.getToggleByName("horoscope", user)
    if (toggle.allow) { //The sample were divided into different groups and used different strategies.
      val experiment = toggle.getExperiment
      if (experiment.getGroupName.equals("treatment_group")) {
        println("I am treatment_group")
        println(experiment.getAllParameters.asScala.mkString(", "))
        println(experiment.getTestKey)
      }
      if (experiment.getGroupName.equals("control_group")) {
        println("I am control_group")
        println(experiment.getAllParameters.asScala.mkString(", "))
        println(experiment.getTestKey)
      }
    } else {
      println("I am original logic")
    }
    Apollo.close()
  }

}
