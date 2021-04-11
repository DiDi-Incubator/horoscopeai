/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.compositor

import java.util

import com.didichuxing.horoscope.core.{Compositor, CompositorFactory}
import com.didichuxing.horoscope.runtime._
import com.didichuxing.horoscope.util.{Logging, PublicLog}
import com.didichuxing.horoscope.runtime.Implicits.gson
import com.typesafe.config.{Config, ConfigFactory}
import com.xiaojukeji.carrera.config.CarreraConfig
import com.xiaojukeji.carrera.producer.CarreraProducer
import com.xiaojukeji.carrera.sd.Env

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

class DDMQCompositorFactory(implicit ec: ExecutionContext) extends CompositorFactory with Logging {

  override def name(): String = "ddmq"

  override def create(code: String)(resource: String => Array[Byte]): Compositor = {
    try {
      val config = ConfigFactory.parseString(code)
      val topic = config.getString("topic")
      val publicLogKey = Try(config.getString("public-log-key")).toOption
      val publicLog = publicLogKey match {
        case Some(key) =>
          Some(new PublicLog(key))
        case None =>
          None
      }
      val ddmqConfig = getDDMQConfig(config)
      val producer = new CarreraProducer(util.Arrays.asList(topic), ddmqConfig)
      producer.start()
      info(("msg", "start ddmq producer"), ("topic", topic))
      Runtime.getRuntime.addShutdownHook(new Thread {
        override def run(): Unit = {
          producer.shutdown()
          info(("msg", "shutdown ddmq producer"), ("topic", topic))
        }
      })
      new DDMQCompositor(producer, topic, publicLog)
    } catch {
      case e: Exception =>
        throw DDMQException(s"Invalid ddmq config ${code}", Some(e.getCause))
    }
  }

  private def getDDMQConfig(params: Config): CarreraConfig = {
    val env = Try(params.getString("env")).getOrElse("test") match {
      case "preview" => Env.Preview
      case "product" => Env.Product
      case _ => Env.Test
    }
    val config = new CarreraConfig(env)
    config.setCarreraProxyTimeout(Try(params.getLong("proxy-timeout")).getOrElse(50))
    config.setCarreraClientTimeout(Try(params.getInt("client-timeout")).getOrElse(100))
    config.setCarreraClientRetry(Try(params.getInt("client-retry")).getOrElse(2))
    config.setCarreraPoolSize(Try(params.getInt("pool-size")).getOrElse(20))
    config.setBatchSendThreadNumber(Try(params.getInt("send-thread")).getOrElse(16))
    config
  }
}

case class DDMQException(message: String, cause: Option[Throwable] = None)
  extends Exception(message, cause.orNull)

class DDMQCompositor(producer: CarreraProducer, topic: String, publicLog: Option[PublicLog])
                    (implicit ec: ExecutionContext) extends Compositor with Logging {

  case class SenderResult(code: Int, msg: String, key: String)

  override def composite(args: ValueDict): Future[Value] = {
    val p = Promise[Value]()
    Future {
      try {
        val bodyDict = args.at("body").getOrElse(args)
        val body = bodyDict.toJson
        val result = producer.send(topic, body)
        val code = result.getCode
        if (code == 0) {
          debug(("msg", "ddmq sender success"), ("body", body))
          if (publicLog.isDefined) {
            bodyDict match {
              case dict: ValueDict =>
                val param = dict.iterator.toSeq.map(v => (v._1.toString, v._2 match {
                  case d: Document =>
                    d.toJson
                  case p: PrimitiveValue[Any] =>
                    p.underlying
                }))
                publicLog.get.public(param: _*)
              case _ =>
                publicLog.get.public(("body", body))
            }
          }
          p.success(Value(SenderResult(result.code, result.msg, result.key)))
        } else {
          error(("msg", "ddmq sender error"), ("result", result))
          p.failure(DDMQException(result.getMsg))
        }
      } catch {
        case ex: Exception =>
          error(("msg", "ddmq sender error"), ("ex", ex.getCause))
          p.failure(ex)
      }
    }
    p.future
  }
}
