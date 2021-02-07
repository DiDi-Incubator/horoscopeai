package com.didichuxing.horoscope.core

import akka.http.scaladsl.server.Route
import com.typesafe.config.Config

trait ConfigStore {

  def getLogConf(): Config

  def getExperimentConf(): Config

  def getSubscriptionConf(): Config

  def register(listener: ConfigChangeListener): Unit

  def api: Route = _.reject()

}

trait ConfigChangeListener {
  def onConfUpdate(): Unit
}
