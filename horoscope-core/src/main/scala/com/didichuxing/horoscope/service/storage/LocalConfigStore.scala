package com.didichuxing.horoscope.service.storage

import com.didichuxing.horoscope.core.{ConfigChangeListener, ConfigStore}
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._
import scala.util.Try

class LocalConfigStore extends ConfigStore {
  private val logConfList = Try(ConfigFactory.load(
    Thread.currentThread().getContextClassLoader, "log"
  ).getConfigList("logs").asScala.toList).getOrElse(Nil)
  private val subscribeConfList = Try(ConfigFactory.load(
    Thread.currentThread().getContextClassLoader, "subscribe"
  ).getConfigList("subscriptions").asScala.toList).getOrElse(Nil)
  private val experimentConfList = Try(ConfigFactory.load(
    Thread.currentThread().getContextClassLoader, "experiment"
  ).getConfigList("experiments").asScala.toList).getOrElse(Nil)

  override def getExperimentConf(name: String): Config = {
    experimentConfList.find(_.getString("name") == name).orNull
  }

  override def getLogConf(name: String): Config = {
    logConfList.find(_.getString("name") == name).orNull
  }

  override def getSubscriptionConf(name: String): Config = {
    subscribeConfList.find(_.getString("name") == name).orNull
  }

  override def getLogConfList: List[Config] = {
    logConfList
  }

  override def getSubscriptionConfList: List[Config] = {
    subscribeConfList
  }

  override def getExperimentConfList: List[Config] = {
    experimentConfList
  }

  override def register(listener: ConfigChangeListener): Unit = {
    null
  }
}
