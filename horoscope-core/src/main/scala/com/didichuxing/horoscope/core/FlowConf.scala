package com.didichuxing.horoscope.core

import com.typesafe.config.Config
import com.didichuxing.horoscope.core.FlowConf._
import com.didichuxing.horoscope.util.FlowConfParser._

class FlowConf(
  val logs: Seq[Config] = Nil, val subscriptions: Seq[Config] = Nil,
  val experiments: Seq[Config] = Nil, val callbacks: Seq[Config] = Nil
) {
  def parseLogConf: Seq[LogConf] = {
    logs.map(_.parseLogConf()).filter(_.enabled)
  }

  def parseSubscribeConf: Seq[SubscribeConf] = {
    subscriptions.map(_.parseSubscribe()).filter(_.enabled)
  }

  def parseCallbackConf: Seq[CallbackConf] = {
    callbacks.map(_.parseCallbackConf()).filter(_.enabled)
  }
}

object FlowConf {

  def apply(): FlowConf = {
    new FlowConf()
  }

  def apply(logs: Seq[Config], subscriptions: Seq[Config],
    experiments: Seq[Config], callbacks: Seq[Config]): FlowConf = {
    new FlowConf(logs, subscriptions, experiments, callbacks)
  }

  case class LogConf(topic: String, enabled: Boolean, flow: String, fields: Seq[LogField]) {
    def expressions: Map[String, Seq[String]] = {
      val result = fields.map(e =>
        (e.flow, e.expression)).groupBy(_._1)
      result.mapValues(v => v.flatMap(_._2).filter(_.nonEmpty))
    }
  }

  case class LogField(name: String, `type`: String, flow: String,
    expression: Option[String], isForward: Boolean)

  case class SubscribeConf(name: String, enabled: Boolean, publisher: String, subscriber: String,
    args: Map[String, String], condition: Option[String], target: Option[String], traffic: Bucket) {

    def expressions: Map[String, Seq[String]] = {
      Map(publisher -> (args.values ++ condition ++ target).toSeq)
    }
  }

  case class CallbackConf(name: String, enabled: Boolean, registerFlow: String, callbackFlow: String,
    timeoutFlow: Option[String], timeout: String, token: String, args: Map[String, String]) {
    def expressions: Map[String, Seq[String]] = {
      Map(registerFlow -> (Seq(token) ++ args.values))
    }
  }

  case class Bucket(lower: Int, upper: Int) {
    def contains(value: Int): Boolean = {
      value >= lower && value < upper
    }

    override def toString: String = {
      s"[${lower},${upper})"
    }
  }

}
