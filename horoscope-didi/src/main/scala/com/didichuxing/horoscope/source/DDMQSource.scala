/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.source

import java.nio.charset.Charset
import java.{lang, util}

import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowEvent
import com.didichuxing.horoscope.core.Sources.EventBuilder
import com.didichuxing.horoscope.core.{EventBus, Source, SourceFactory}
import com.didichuxing.horoscope.runtime.Value
import com.didichuxing.horoscope.runtime.convert.ValueTypeAdapter
import com.didichuxing.horoscope.util.Utils._
import com.didichuxing.horoscope.util.{Logging, PBParserUtil, SystemLog}
import com.xiaojukeji.carrera.consumer.thrift.{Context, Message}
import com.xiaojukeji.carrera.consumer.thrift.client.BaseMessageProcessor.Result
import com.xiaojukeji.carrera.consumer.thrift.client.BaseMessageProcessor.Result._
import com.xiaojukeji.carrera.consumer.thrift.client.{BatchMessageProcessor, CarreraConfig, CarreraConsumer}
import com.xiaojukeji.carrera.sd.Env
import com.google.gson.GsonBuilder
import com.typesafe.config.Config

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class DDMQSourceFactory(builder: EventBuilder[util.List[Message], List[Value]]) extends SourceFactory {

  override def newSource(params: Config): Source = new DDMQSource(params, builder)

}

class DDMQPBSourceFactory(pbBuilder: String => EventBuilder[util.List[Message], List[Value]]) extends SourceFactory {
  override def newSource(params: Config): Source = {
    val className = params.getString("class-name")
    if (PBParserUtil.containsMessageType(className)) {
      new DDMQSource(params, pbBuilder(className))
    } else {
      throw new IllegalAccessException(s"Class name: ${className} doesn't exist")
    }
  }
}


object DDMQSourceBuilder {
  private val gson = new GsonBuilder()
    .registerTypeHierarchyAdapter(classOf[Value], new ValueTypeAdapter)
    .setPrettyPrinting()
    .create()

  def builder(): EventBuilder[util.List[Message], List[Value]] = {
    messages => {
      val events = ListBuffer[Value]()
      for (msg <- messages) {
        val data = msg.getValue
        val json = new String(data, Charset.forName("UTF-8"))
        val value = gson.fromJson(json, classOf[Value])
        events.append(value)
      }
      events.toList
    }
  }

  def pbEventBuilder(className: String): EventBuilder[util.List[Message], List[Value]] = {
    messages => {
      val events = ListBuffer[Value]()
      for (msg <- messages) {
        val data = msg.getValue
        val message = PBParserUtil.parseMessage(className, data)
        events.append(Value(message))
      }
      events.toList
    }
  }

}

/**
 * ddmq数据源，默认将数据统一转换为List[Value]
 *
 * @param sourceConfig 配置
 * @param builder      源数据构造函数
 */
class DDMQSource(sourceConfig: Config, builder: EventBuilder[util.List[Message], List[Value]])
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
    consumer.startConsume(new BatchMessageProcessor {
      override def process(messages: util.List[Message], context: Context): util.Map[Result, util.List[lang.Long]] = {
        SystemLog.create()
        val beginTime = System.currentTimeMillis()
        try {
          info(("msg", "receive message"), ("source", sourceName), ("count", messages.size()))
          val values = builder(messages)
          //成功消费的消息
          val successEvents = eventBus.process(values)
          commitOffset(messages, successEvents)
        } catch {
          case ex: Exception =>
            ex.printStackTrace()
            error(("msg", s"ddmq pull message error, ${ex.getMessage}"))
            //全部回滚
            rollbackOffset(messages)
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

  /**
   * 计算成功和失败的offset
   *
   * @return
   */
  private def commitOffset(messages: util.List[Message],
                           successEvents: List[FlowEvent]): util.Map[Result, util.List[lang.Long]] = {
    val results = new util.HashMap[Result, util.List[lang.Long]]()
    results.put(SUCCESS, new util.ArrayList[lang.Long]())
    results.put(FAIL, new util.ArrayList[lang.Long]())
    for ((message, i) <- messages.zipWithIndex) {
      val event = successEvents.get(i)
      if (event != null) {
        results.get(SUCCESS).append(message.getOffset)
      } else {
        results.get(FAIL).append(message.getOffset)
      }
    }
    info(("msg", "commit events"), ("source", sourceName), ("successCount", results.get(SUCCESS).size()),
      ("failCount", results.get(FAIL).size()))
    results
  }

  private def rollbackOffset(messages: util.List[Message]): util.Map[Result, util.List[lang.Long]] = {
    val results = new util.HashMap[Result, util.List[lang.Long]]()
    results.put(FAIL, new util.ArrayList[lang.Long]())
    for (message <- messages) {
      results.get(FAIL).append(message.getOffset)
    }
    results
  }
}
