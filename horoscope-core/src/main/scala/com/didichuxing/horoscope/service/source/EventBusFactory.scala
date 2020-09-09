/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import java.util.concurrent.Executor

import com.didichuxing.horoscope.core.EventBus
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.service.resource.Participant
import com.typesafe.config.Config

import scala.util.Try

object EventBusFactory {
  def newEventBus(sourceName: String, flowName: String, params: Config)
                 (implicit ctx: ApplicationContext): EventBus = {
    val rpcType = Try(params.getString("rpc.type")).getOrElse("default")
    rpcType match {
      case "grpc" =>
        new GRPCEventBus(sourceName, flowName, params)
      case _ =>
        new DefaultEventBus(sourceName, flowName, params)
    }
  }

  def newClient(participant: Participant, params: Config)
               (implicit ec: Executor): EventProcessorClient = {
    val rpcType = Try(params.getString("rpc.type")).getOrElse("default")
    rpcType match {
      case "grpc" =>
        new GRPCEventProcessorClient(participant, params)
      case _ =>
        null
    }
  }
}
