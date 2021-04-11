package com.didichuxing.horoscope.util

import com.didichuxing.horoscope.core.Flow.{Bucket, LogConf, AssignField, SubscribeConf}
import com.didichuxing.horoscope.runtime.experiment.ABTestController.{ABTestConf, ABTestGroup}
import com.didichuxing.horoscope.runtime.{Implicits, Value}
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.util.Try

object FlowConfParser {
  implicit class LogConfParser(conf: Config) {
    def parseLogConf(): LogConf = {
      val flow = conf.getString("flow")
      val enabled = Try(conf.getBoolean(ENABLED)).getOrElse(false)
      val fields = if (conf.hasPath(ASSIGN)) {
        conf.getConfigList(ASSIGN).asScala.map({ assign =>
          val flow = assign.getString("flow")
          val name = assign.getString("name")
          val expr = assign.getString(EXPRESSION)
          AssignField(flow, name, expr)
        })
      } else {
        Nil
      }

      val choices = if (conf.hasPath(CHOICE)) conf.getStringList(CHOICE).asScala else Nil

      LogConf(flow, enabled, fields, choices)
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
      val flow = conf.getString("flow")
      val condition = if (conf.getString(TRAFFIC_CONDITION).nonEmpty) Some(conf.getString(TRAFFIC_CONDITION)) else None
      val target = if (conf.getString(TRAFFIC_TARGET).nonEmpty) Some(conf.getString(TRAFFIC_TARGET)) else None
      val groups = if (conf.hasPath(EXPERIMENT_GROUPS)) {
        conf.getConfigList(EXPERIMENT_GROUPS).asScala.map({ group =>
          val groupName = group.getString("name")
          val flow = group.getString("flow")
          val bucket = group.getIntList(TRAFFIC_BUCKET).asScala.map(_.intValue())
          assert(bucket.length == 2, "bucket range is not valid")
          val lower = bucket.head
          val upper = bucket.last
          val params = if (group.hasPath(FLOW_ARGS)) {
            group.getConfigList(FLOW_ARGS).asScala.map({ p =>
              val name = p.getString("name")
              val value = p.getString("value")
              name -> Implicits.gson.fromJson(value, classOf[Value])
            }).toMap
          } else {
            Map.empty[String, Value]
          }

          ABTestGroup(groupName, flow, params, Bucket(lower, upper))
        })
      } else {
        Nil
      }

      ABTestConf(name, enabled, flow, condition, target, groups)
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
}
