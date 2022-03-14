package com.didichuxing.horoscope.service.storage

import com.didichuxing.horoscope.core.{ConfigChangeListener, ConfigChecker, ConfigStore}
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
  private val callbackConfList = Try(ConfigFactory.load(
    Thread.currentThread().getContextClassLoader, "callback"
  ).getConfigList("callbacks").asScala.toList).getOrElse(Nil)

  import com.didichuxing.horoscope.util.Constants._

  override def getConf(name: String, confType: String): Config = {
    confType match {
      case LOG_TYPE => logConfList.find(_.getString("name") == name).orNull
      case SUBSCRIPTION_TYPE => subscribeConfList.find(_.getString("name") == name).orNull
      case EXPERIMENT_TYPE => experimentConfList.find(_.getString("name") == name).orNull
      case CALLBACK_TYPE => callbackConfList.find(_.getString("name") == name).orNull
      case _ => throw new IllegalArgumentException
    }
  }

  override def getConfList(confType: String): List[Config] =  {
    confType match {
      case LOG_TYPE => logConfList
      case SUBSCRIPTION_TYPE => subscribeConfList
      case EXPERIMENT_TYPE => experimentConfList
      case CALLBACK_TYPE => callbackConfList
      case _ => throw new IllegalArgumentException
    }
  }

  override def registerListener(listener: ConfigChangeListener): Unit = {
    null
  }

  override def registerChecker(checker: ConfigChecker): Unit = {
    null
  }
}
