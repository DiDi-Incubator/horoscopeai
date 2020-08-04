/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.source

import java.nio.charset.Charset
import java.util

import com.didichuxing.horoscope.core.Sources.EventBuilder
import com.didichuxing.horoscope.core.{EventBus, Source, SourceFactory}
import com.didichuxing.horoscope.runtime.Value
import com.didichuxing.horoscope.util.Utils._
import com.didichuxing.horoscope.util.{Logging, SystemLog}
import com.xiaojukeji.carrera.consumer.thrift.{Context, Message}
import com.xiaojukeji.carrera.consumer.thrift.client.BaseMessageProcessor.Result
import com.xiaojukeji.carrera.consumer.thrift.client.BaseMessageProcessor.Result._
import com.xiaojukeji.carrera.consumer.thrift.client._
import com.xiaojukeji.carrera.sd.Env
import com.typesafe.config.Config

import scala.collection.JavaConversions._

class DDMQSingleSourceFactory(builder: EventBuilder[util.List[Message], List[Value]]) extends SourceFactory {

  override def newSource(params: Config): Source = new DDMQSingleSource(params, builder)

}

/**
 * ddmq数据源，默认将数据统一转换为List[Value]
 *
 * @param sourceConfig 配置
 * @param builder      源数据构造函数
 */
class DDMQSingleSource(sourceConfig: Config, builder: EventBuilder[util.List[Message], List[Value]])
  extends Source with Logging {

  val sourceName = sourceConfig.getString("source-name")
  val params = sourceConfig.getConfig("parameter")
  var eventBus: EventBus = _
  var consumer: CarreraConsumer = _

  override def start(eventBus: EventBus): Unit = {
    this.eventBus = eventBus
    eventBus.start()
    pull()
  }

  override def stop(): Unit = {
    if (consumer != null) {
      consumer.stop()
    }
    if (eventBus != null) {
      eventBus.stop()
    }
    debug(("msg", "stop ddmq source"))
  }

  def pull() {
    debug(("msg", "ddmq begin pulling"))
    consumer = new CarreraConsumer(ddmqConfig())
    consumer.startConsume(new MessageProcessor {
      override def process(message: Message, context: Context): Result = {
        SystemLog.create()
        val beginTime = System.currentTimeMillis()
        val messages = List(message)
        try {
          info(("msg", "receive message"), ("source", sourceName), ("count", messages.size()))
          val values = builder(messages)
          //成功消费的消息
          val successEvents = eventBus.process(values)
          if (successEvents.size == 1 && successEvents(0) != null) {
            SUCCESS
          } else {
            error(("msg", "ddmq process message error"), ("message", message))
            FAIL
          }
        } catch {
          case ex: Throwable =>
            ex.printStackTrace()
            error(("msg", s"ddmq pull message error, ${ex.getMessage}"), ("ex", ex.getCause))
            FAIL
        } finally {
          val endTime = System.currentTimeMillis()
          info(("msg", "total time"), ("source", sourceName), ("proc_time", s"${endTime - beginTime}ms"))
        }
      }
    }, configIntOrDefault(params, "ddmq.concurrency", 1))
  }

  private def ddmqConfig(): CarreraConfig = {
    val env = params.getString("ddmq.env") match {
      case "preview" => Env.Preview
      case "product" => Env.Product
      case _ => Env.Test
    }
    val group = params.getString("ddmq.group")
    val config = new CarreraConfig(group, env)
    config.setRetryInterval(100) // 拉取消息重试的间隔时间，单位ms。
    config.setMaxBatchSize(configIntOrDefault(params, "ddmq.max", 1)) //一次拉取的消息数量。
    debug(("msg", "ddmq config"), ("config", config), ("params", params))
    config
  }
}
