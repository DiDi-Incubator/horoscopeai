/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: liangguorong@didiglobal.com
 */

package com.didichuxing.horoscope.logging

import java.util.{Properties, UUID}

import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowInstance
import com.didichuxing.horoscope.core.OdsLogger
import com.didichuxing.horoscope.util.Logging
import com.typesafe.config.Config
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{BytesSerializer, StringSerializer}
import org.apache.kafka.common.utils.Bytes

import scala.util.Try

class KafkaOdsLogger(config: Config) extends OdsLogger with LogHelper with Logging {
  import scala.collection.JavaConverters._
  private val producerConfig = getProducerConfig(config.getConfig("horoscope.ods-logger.kafka"))
  private val producer = new KafkaProducer[String, Bytes](producerConfig)
  private val topic = Try(config.getString("horoscope.ods-logger.kafka.topic")).getOrElse("")
  assert(topic.size > 0, "topic is null")
  info(("msg", "KafkaOdsLogger started"))

  private val topicLogger = new KafkaTopicLogger(config)

  override def log(flowInstance: FlowInstance): Unit = {
    try {
      topicLogger.log(flowInstance)
      val transLog = Bytes.wrap(flowInstance.toByteArray)
      val record = new ProducerRecord[String, Bytes](topic, null,
        System.currentTimeMillis(),
        UUID.randomUUID().toString, transLog)
      producer.send(record, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (exception != null) {
            logError(("msg", s"sent ods log error"), ("topic", topic),
              ("metadata", metadata.toString), ("logdata", flowInstance.toJson()), ("ex", exception.toString))
          } else {
            logDebug(("msg", s"Successfully send event to kafka"), ("topic", topic), ("logdata", transLog))
          }
        }
      })
    } catch {
      case e: Throwable =>
        logError(("msg", s"sent ods log error"), ("topic", topic),
          ("logdata", flowInstance.toJson()), ("ex", e.toString))
    }
  }

  private def getProducerConfig(kafkaConfig: Config): Properties = {
    try {
      val properties = new Properties
      kafkaConfig.entrySet().asScala.foreach { entry =>
        properties.setProperty(entry.getKey, entry.getValue.unwrapped().toString)
        info(("msg", "kafka ods logger config"), (entry.getKey, entry.getValue.unwrapped().toString))
      }
      properties.setProperty("key.serializer", classOf[StringSerializer].getCanonicalName)
      properties.setProperty("value.serializer", classOf[BytesSerializer].getCanonicalName)
      properties
    } catch {
      case e: Exception =>
        throw new RuntimeException("Can't init kafka ods logger", e)
    }
  }

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run(): Unit = {
      try {
        if (producer != null) {
          producer.close()
          logInfo(("msg", "close ods kafka producer"), ("topic", topic))
        }
      } catch {
        case e: Throwable =>
          e.printStackTrace()
          logError(("msg", "close ods kafka producer error"), ("ex", e.toString))
      }
    }
  })

}
