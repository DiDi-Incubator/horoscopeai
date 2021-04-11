package com.didichuxing.horoscope.compositor

import com.didichuxing.horoscope.core.{Compositor, CompositorFactory}
import com.didichuxing.horoscope.runtime.{Text, Value, ValueDict}
import com.didichuxing.horoscope.util.Logging
import com.didichuxing.horoscope.runtime.Implicits.gson
import com.typesafe.config.ConfigFactory
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.concurrent.{ExecutionContext, Future, Promise}

class KafkaCompositor(producer: KafkaProducer[Array[Byte], Array[Byte]], topic: String) (implicit ec: ExecutionContext)
  extends Compositor with Logging {

  override def composite(args: ValueDict): Future[Value] = {
    val p = Promise[Value]()
    Future {
      try {
        val key = args.at("key").getOrElse(Text("0")).as[Text].underlying.getBytes("UTF-8")
        val value = args.at("value").getOrElse(args).toJson.getBytes("UTF-8")
        val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, key, value)
        producer.send(record)
        p.success(Value(true))
      } catch {
        case ex: Exception =>
          error(("msg", "kafka sender error"), ("ex", ex.getCause))
          p.failure(ex)
      }
    }
    p.future
  }
}

case class KafkaException(message: String, cause: Option[Throwable] = None)
  extends Exception(message, cause.orNull)

class KafkaCompositorFactory(implicit ec: ExecutionContext) extends CompositorFactory with Logging {

  val format = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s.%s\" password=\"%s\";"

  override def name(): String = "kafka"

  override def create(code: String)(resource: String => Array[Byte]): Compositor = {
    try {
      val config = ConfigFactory.parseString(code)
      val clusterId = config.getString("cluster-id")
      val servers = config.getString("servers")
      val topic = config.getString("topic")
      val appId = config.getString("app-id")
      val appPwd = config.getString("password")
      info(s"Kafka config, clusterId: $clusterId, servers: $servers, topic: $topic, appId: $appId, appPwd: $appPwd")

      val properties = getProducerProperties(clusterId, servers, appId, appPwd)
      val producer = new KafkaProducer[Array[Byte], Array[Byte]](properties)
      Runtime.getRuntime.addShutdownHook(new Thread {
        override def run(): Unit = {
          producer.close()
          info(("msg", "close kafka producer"), ("topic", topic))
        }
      })
      new KafkaCompositor(producer, topic)
    } catch {
      case e: Exception =>
        throw KafkaException(s"Invalid kafka config $code", Some(e.getCause))
    }
  }

  def getProducerProperties(clusterId: String, servers: String, appId: String, appPwd: String): Properties = {
    val properties = getAuthorizedProperties(clusterId, appId, appPwd)
    properties.put("bootstrap.servers", servers)
    properties.put("max.request.size", (1024 * 1024 * 10).toString)
    properties.put("compression.type", "lz4")
    properties.put("batch.size", "1048576")
    properties.put("linger.ms", "1000")
    properties.put("retries", "10")
    val byteArraySerializer = "org.apache.kafka.common.serialization.ByteArraySerializer"
    properties.put("key.serializer", byteArraySerializer)
    properties.put("value.serializer", byteArraySerializer)
    properties
  }

  def getAuthorizedProperties(clusterId: String = "", appId: String, appPwd: String): Properties = {
    val properties = new Properties()
    if (clusterId.nonEmpty) {
      properties.put("security.protocol", "SASL_PLAINTEXT")
      properties.put("sasl.mechanism", "PLAIN")
      val jaasConfig = String.format(format, clusterId, appId, appPwd)
      properties.put("sasl.jaas.config", jaasConfig)
    }
    properties
  }

}
