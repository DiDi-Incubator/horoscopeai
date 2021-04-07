package com.didichuxing.horoscope.core

import akka.http.scaladsl.server.Route
import com.typesafe.config.Config

trait ConfigStore {

  def getLogConf(name: String): Config

  def getExperimentConf(name: String): Config

  def getSubscriptionConf(name: String): Config

  def getLogConfList: List[Config]

  def getExperimentConfList: List[Config]

  def getSubscriptionConfList: List[Config]

  def register(listener: ConfigChangeListener): Unit

  def api: Route = _.reject()
}

trait ConfigChangeListener {
  def onConfUpdate(): Unit
}
