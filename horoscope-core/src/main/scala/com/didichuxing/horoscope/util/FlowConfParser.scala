package com.didichuxing.horoscope.util

import com.didichuxing.horoscope.core.FlowConf._
import com.didichuxing.horoscope.runtime.experiment.ABTestController._
import com.didichuxing.horoscope.runtime.experiment.ExperimentController.FlowOption
import com.didichuxing.horoscope.runtime.{Implicits, Value}
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.util.Try

object FlowConfParser {

  implicit class LogConfParser(conf: Config) {
    def parseLogConf(): LogConf = {
      val topic = conf.getString("name")
      val enabled = Try(conf.getBoolean(ENABLED)).getOrElse(false)
      val detailed = Try(conf.getBoolean("detailed")).getOrElse(false)
      val flow = conf.getString("flow")
      val fields = if (conf.hasPath("fields")) {
        conf.getConfigList(FIELDS).asScala.map({ field =>
          val name = field.getString("name")
          val `type` = field.getString("type")
          val flow = field.getString("meta.flow")
          val isForward = field.getBoolean("meta.forward")
          val expr = `type` match {
            case FLOW_VARIABLE_TYPE => Some(field.getString("meta."  + EXPRESSION))
            case FLOW_TAG_TYPE => None
            case FLOW_CHOICE_TYPE => None
          }
          LogField(name, `type`, flow, expr, isForward)
        })
      } else {
        Nil
      }

      LogConf(topic, enabled, flow, fields, detailed)
    }
  }

  implicit class SubscribeConfParser(conf: Config) {
    def parseSubscribe(): SubscribeConf = {
      val name = conf.getString("name")
      val enabled = Try(conf.getBoolean(ENABLED)).getOrElse(false)
      val publisher = conf.getString(PUBLISHER_FLOW)
      val subscriber = conf.getString(SUBSCRIBER_FLOW)
      val condition = if (conf.getString(TRAFFIC_CONDITION).nonEmpty) Some(conf.getString(TRAFFIC_CONDITION)) else None
      val target = if (conf.getString(TRAFFIC_TARGET).nonEmpty) Some(conf.getString(TRAFFIC_TARGET)) else None
      val bucket = conf.getIntList(TRAFFIC_BUCKET).asScala.map(_.intValue())
      assert(bucket.length == 2, "bucket range is not valid")
      val lower = bucket.head
      val upper = bucket.last
      val args = if (conf.hasPath(FLOW_ARGS)) {
        conf.getConfigList(FLOW_ARGS).asScala.map({ p =>
          val key = p.getString("name")
          val expr = p.getString(EXPRESSION)
          key -> expr
        }).toMap
      } else {
        Map[String, String]()
      }

      SubscribeConf(name, enabled, publisher, subscriber, args, condition, target, Bucket(lower, upper))
    }
  }

  implicit class ABTestConfParser(conf: Config) {
    def parseABTestConf(): ABTestConf = {
      val name = conf.getString("name")
      val enabled = Try(conf.getBoolean(ENABLED)).getOrElse(false)
      val mainFlow = conf.getString("flow")
      val priority = Try(conf.getInt("priority")).getOrElse(1)
      val condition = if (conf.getString(TRAFFIC_CONDITION).nonEmpty) Some(conf.getString(TRAFFIC_CONDITION)) else None
      val target = if (conf.getString(TRAFFIC_TARGET).nonEmpty) Some(conf.getString(TRAFFIC_TARGET)) else None
      val bucket = conf.getIntList("traffic").asScala.map(_.intValue())
      val traffic = Bucket(bucket.head, bucket.last)
      val groups = if (conf.hasPath(EXPERIMENT_GROUPS)) {
        conf.getConfigList(EXPERIMENT_GROUPS).asScala.map({ group =>
          val groupName = group.getString("name")
          val bucket = group.getIntList(TRAFFIC_BUCKET).asScala.map(_.intValue())
          val exprBucket = Bucket(bucket.head, bucket.last)
          val flows = group.getConfigList("flows").asScala.map { flowConf =>
            val original = flowConf.getString("original")
            val replaced = flowConf.getString("new")
            val params = if (flowConf.hasPath(FLOW_ARGS)) {
              flowConf.getConfigList(FLOW_ARGS).asScala.map({ p =>
                val name = p.getString("name")
                val value = p.getString("value")
                name -> value
              }).toMap
            } else {
              Map.empty[String, String]
            }

            original -> FlowOption(replaced, params)
          }.toMap
          ABTestGroup(groupName, exprBucket, flows)
        })
      } else {
        Nil
      }

      ABTestConf(name, enabled, mainFlow, condition, target, groups, priority, traffic)
    }
  }

  implicit class CallbackConfParser(conf: Config) {
    def parseCallbackConf(): CallbackConf = {
      val name = conf.getString("name")
      val enabled = conf.getBoolean("enabled")
      val registerFlow = conf.getString("flow.register")
      val callbackFlow = conf.getString("flow.callback")
      val timeoutFlow = Try(Some(conf.getString("flow.timeout"))).getOrElse(None)
      val timeout = conf.getString("timeout")
      val token = conf.getString("token")
      val args = if (conf.hasPath(FLOW_ARGS)) {
        conf.getConfigList(FLOW_ARGS).asScala.map({ p =>
          val key = p.getString("name")
          val expr = p.getString(EXPRESSION)
          key -> expr
        }).toMap
      } else {
        Map.empty[String, String]
      }

      CallbackConf(name, enabled, registerFlow, callbackFlow, timeoutFlow, timeout, token, args)
    }
  }

  val FLOW_ARGS = "args"
  val EXPERIMENT_GROUPS = "groups"
  val TRAFFIC_CONDITION = "condition"
  val TRAFFIC_TARGET = "target"
  val TRAFFIC_BUCKET = "bucket"
  val PUBLISHER_FLOW = "publisher"
  val SUBSCRIBER_FLOW = "subscriber"
  val ASSIGN = "assign"
  val CHOICE = "choice"
  val EXPRESSION = "expression"
  val ENABLED = "enabled"
  val FIELDS = "fields"
  val VARIABLE = "variable"
  val FLOW_TAG_TYPE = "tag"
  val FLOW_VARIABLE_TYPE = "variable"
  val FLOW_CHOICE_TYPE = "choice"
}
