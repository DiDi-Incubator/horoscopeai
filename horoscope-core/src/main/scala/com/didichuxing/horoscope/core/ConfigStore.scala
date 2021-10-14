package com.didichuxing.horoscope.core

import akka.http.scaladsl.server.Route
import com.typesafe.config.Config

trait ConfigStore {

  def getConf(name: String, confType: String): Config

  def getConfList(confType: String): List[Config]

  def registerListener(listener: ConfigChangeListener): Unit

  def registerChecker(checker: ConfigChecker): Unit

  def api: Route = _.reject()
}

trait ConfigChangeListener {
  def onConfUpdate(): Unit
}

trait ConfigChecker {
  @throws(classOf[AssertionError])
  def check(name: String, configType: String, conf: Config): Boolean
}
