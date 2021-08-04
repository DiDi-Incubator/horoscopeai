package com.didichuxing.horoscope.logging

import java.util.{Properties, UUID}

import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowInstance
import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowInstance.Topic
import com.didichuxing.horoscope.runtime.Value
import com.didichuxing.horoscope.util.Logging
import com.typesafe.config.Config
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

class TopicLogger(config: Config) extends Logging with LogHelper {
  import scala.collection.JavaConverters._
  private val kafkaConfig = config.getConfig("horoscope.topic-logger.kafka")
  private val topic: String = kafkaConfig.getString("topic")
  private val producerConfig = getProducerConfig(kafkaConfig)
  private val producer = new KafkaProducer[String, String](producerConfig)

  info(("msg", "TopicLogger started"))

  def log(flowInstance: FlowInstance): Unit = {
    flowInstance.getTopicList.asScala.foreach { t =>
      sendRecord(t)
    }
  }

  private def sendRecord(topicRecord: Topic): Unit = {
    val jsonLog = Value(topicRecord).toJson
    val record = new ProducerRecord[String, String](topic, UUID.randomUUID().toString, jsonLog)
    producer.send(record, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception != null) {
          logError(("msg", s"Failed to send topic log "), ("topic", topic),
            ("metadata", metadata.toString), ("logdata", jsonLog), ("ex", exception.toString))
        } else {
          logDebug(("msg", s"Successfully send topic log to kafka"), ("topic", topic), ("logdata", jsonLog))
        }
      }
    })
  }

  private def getProducerConfig(kafkaConfig: Config): Properties = {
    try {
      val properties = new Properties
      kafkaConfig.entrySet().asScala.foreach { entry =>
        properties.setProperty(entry.getKey, entry.getValue.unwrapped().toString)
        info(("msg", "topic-log logger config"), (entry.getKey, entry.getValue.unwrapped().toString))
      }
      properties.setProperty("key.serializer", classOf[StringSerializer].getCanonicalName)
      properties.setProperty("value.serializer", classOf[StringSerializer].getCanonicalName)
      properties
    } catch {
      case e: Throwable =>
        throw new RuntimeException("Can't init topic logger", e)
    }
  }

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run(): Unit = {
      try {
        if (producer != null) {
          producer.close()
          logInfo(("msg", "close topic-log kafka producer"), ("topic", topic))
        }
      } catch {
        case e: Throwable =>
          e.printStackTrace()
          logError(("msg", "close topic-log kafka producer error"), ("ex", e.toString))
      }
    }
  })

}
