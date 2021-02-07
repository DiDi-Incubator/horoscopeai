package com.didichuxing.horoscope.service.storage

import com.didichuxing.horoscope.core.{ConfigChangeListener, ConfigStore}
import com.typesafe.config.Config

class ZookeeperConfigStore extends ConfigStore {
  override def getLogConf(): Config = ???

  override def getExperimentConf(): Config = ???

  override def getSubscriptionConf(): Config = ???

  override def register(listener: ConfigChangeListener): Unit = ???
}
