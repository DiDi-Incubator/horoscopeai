/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import java.util.concurrent.ConcurrentHashMap
import java.util.function.Function

import com.didichuxing.horoscope.core.FlowRuntimeMessage._
import com.didichuxing.horoscope.core.Sources.EventBuilder
import com.didichuxing.horoscope.runtime.{ValueDict, _}
import com.didichuxing.horoscope.util.Utils._
import com.didichuxing.horoscope.util.{Constants, Logging, PBParserUtil}
import com.didichuxing.horoscope.runtime.expression.{BuiltIn, Expression}
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import scala.util.Try
import Implicits.gson

/**
 * 定义了所有的原始source消息转换过程
 */
object EventBuilders extends Logging {

  private val expressionCache = new ConcurrentHashMap[String, Expression]()

  def jsonEventBuilder(): EventBuilder[String, Value] = {
    json => {
      gson.fromJson(json, classOf[Value])
    }
  }

  /**
   * 将某个数据源的消息转换为统一的SourceEvent结构，不同格式的消息源定义不同方法
   * 如果都是kafka消息，只是转换方法不一致，通过定义不同的builder即可
   *
   * @return
   */
  def sourceEventBuilder(): EventBuilder[List[ConsumerRecord[String, String]], List[Value]] = {
    records => {
      info(("msg", "build source event"), ("record count", records.size))
      val events = ListBuffer[Value]()
      for (record <- records) {
        val json = record.value()
        val value = gson.fromJson(json, classOf[Value])
        events.append(value)
      }
      events.toList
    }
  }

  def sourcePBEventBuilder(className: String): EventBuilder[List[ConsumerRecord[String, Array[Byte]]], List[Value]] = {
    records => {
      info(("msg", "build source event"), ("record count", records.size))
      val events = ListBuffer[Value]()
      for (record <- records) {
        val bytes = record.value()
        val value = Value(PBParserUtil.parseMessage(className, bytes))
        events.append(value)
      }
      events.toList
    }
  }

  /**
   * 将消息源转换为FlowEvent
   *
   * @param sourceEvents
   * @param sourceName
   * @param flowName
   * @return
   */
  def flowEventBuilder(sourceEvents: List[Value], sourceName: String, flowName: String,
                       params: Config)(implicit buildIn: BuiltIn): List[SeqFlowEvent] = {
    val flowEvents = ListBuffer[SeqFlowEvent]()
    for ((value, i) <- sourceEvents.zipWithIndex) {
      if (value.isInstanceOf[ValueDict]) {
        var dictValue = extractEvent(sourceName, params, value.asInstanceOf[ValueDict])
        //每次生成一个全新的eventId
        val eventId = Text(getEventId)
        dictValue = dictValue.updated(Constants.EVENT_ID, eventId)
        //获取traceId
        val traceId = dictValue.visit(Constants.TRACE_ID) match {
          case NULL =>
            val traceField = Try(params.getString("trace-field"))
            if (traceField.isSuccess) {
              Try(dictValue.eval(traceField.get)).getOrElse(NULL) match {
                case NULL =>
                  //trace value not found
                  Text(getTraceId(eventId.underlying))
                case v@NumberValue(_) =>
                  Text(v.toString)
                case v@Text(_) =>
                  v
                case rawTraceId@_ =>
                  //trace value
                  Text(rawTraceId.toString)
              }
            } else {
              //no trace field
              Text(getTraceId(eventId.underlying))
            }
          case sourceTraceId@_ =>
            //source traceId
            sourceTraceId
        }
        dictValue = dictValue.updated(Constants.TRACE_ID, traceId)
        //抽取dictValue
        val flowEvent = FlowEvent.newBuilder()
          .setEventId(eventId.underlying)
          .setFlowName(flowName)
          .setTraceId(traceId.asInstanceOf[Text].underlying)
          .putArgument("@", TraceVariable.newBuilder().setValue(dictValue.as[FlowValue]).build())
        flowEvents.append(SeqFlowEvent(i, flowEvent.build()))
      } else {
        //格式错误
        flowEvents.append(SeqFlowEvent(i, null))
        error(("msg", "value format error"), ("value", value))
      }
    }
    flowEvents.toList
  }

  // scalastyle:off
  private def extractEvent(sourceName: String, params: Config, dictValue: ValueDict)
                          (implicit buildIn: BuiltIn): ValueDict = {
    if (params.hasPath("extract-expression")) {
      val expression = getExpression(sourceName, params)
      Try(expression.apply(dictValue)).getOrElse(NULL) match {
        case event: ValueDict =>
          event
        case _ =>
          warn(("msg", s"extract event error: ${dictValue.toJson(Implicits.gson)}"), ("expression", expression))
          dictValue
      }
    } else {
      dictValue
    }
  }

  private def getExpression(sourceName: String, params: Config)(implicit buildIn: BuiltIn): Expression = {
    expressionCache.computeIfAbsent(sourceName, new Function[String, Expression]() {
      override def apply(t: String): Expression = {
        val extract = params.getConfig("extract-expression").entrySet()
          .filter(_.getKey.trim.nonEmpty)
          .map(v => s"${"\"" + v.getKey + "\""}:${v.getValue.unwrapped()}").mkString(",")
        Expression(s"{$extract}")
      }
    })
  }

  /**
   * bytes -> BinaryValue
   * scheduler source builder
   *
   * @return
   */
  def schedulerSourceEventBuilder(): EventBuilder[List[Array[Byte]], List[Value]] = {
    records => {
      info(("msg", "build source event"), ("record count", records.size))
      val events = ListBuffer[Value]()
      for (record <- records) {
        val value = Binary(record)
        events.append(value)
      }
      events.toList
    }
  }

  /**
   * BinaryValue -> FlowEvent
   *
   * @param sourceEvents
   * @return
   */
  def schedulerFlowEventBuilder(sourceEvents: List[Value]): List[FlowEvent] = {
    val flowEvents = ListBuffer[FlowEvent]()
    for (raw <- sourceEvents) {
      val binary = raw.asInstanceOf[Binary]
      val event = FlowEvent.parseFrom(binary.underlying)
      flowEvents.append(event)
    }
    flowEvents.toList
  }

}
