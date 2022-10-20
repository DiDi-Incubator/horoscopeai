/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.mock

import java.util.Properties

import com.google.common.util.concurrent.RateLimiter
import com.google.gson.GsonBuilder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

case class Body(x: String, y: String)
case class SourceEvent(traceId: String, eventId: String, value: String, body: Body)
object KafkaProducer extends App {
  val props = new Properties();
  props.put("bootstrap.servers", "127.0.0.1:9093")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val gson = new GsonBuilder().create()
  val producer = new KafkaProducer[String, String](props)
  val rateLimiter = RateLimiter.create(1)
  while (true) {
    rateLimiter.acquire()
    val x = Random.nextInt()
    val key = "key" + x
    val value = "value" + x
    val event = SourceEvent(null, null, value, Body("a", "b"))
    val json = gson.toJson(event)
    producer.send(new ProducerRecord[String, String]("event_source", key, json)).get()
  }
  producer.close()
}