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
  //请填写正确的bootstrap server地址
  props.put("bootstrap.servers", "10.179.24.223:9093")//填写上文服务发现地址
  props.put("compression.type", "lz4") //压缩方式，在平衡存储与cpu使用率后推荐使用lz4
  props.put("linger.ms", "500") // 建议500,务必要改
  props.put("batch.size", "100000")//每个请求的批量大小
  props.put("max.in.flight.requests.per.connection", "1") //如果需要保证消息顺序，需要设置为1，默认为5
  props.put("security.protocol", "SASL_PLAINTEXT")//注意安全管控的security.protocol内容
  props.put("sasl.mechanism", "PLAIN")//注意安全管控的security.protocol内容
  //请填写正确的clusterId，appId，密码  clusterId对应关系见上表
  val format = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s.%s\" password=\"%s\";"
  val clusterId = "95"
  val appId = "appId_001485"
  val password = "_twYiKJNrBCX"
  val jaas_config = String.format(format, {clusterId}, {appId}, {password})
  props.put("sasl.jaas.config", jaas_config);
  //根据实际场景选择序列化类
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
    //println(json)
    val beginTime = System.currentTimeMillis()
    val result1 = producer.send(new ProducerRecord[String, String]("event_source", key, json)).get()
    val endTime = System.currentTimeMillis()
    println(s"$result1::${endTime - beginTime}ms")
    //val result2 = producer.send(new ProducerRecord[String, String]("eventSource", key, "json")).get()
    //println(result2)
  }
  producer.close()
}