/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import java.util
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Callable, Future, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import com.alibaba.ttl.threadpool.TtlExecutors
import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowEvent
import com.didichuxing.horoscope.core.Sources.EventBuilder
import com.didichuxing.horoscope.core.{EventBus, Source}
import com.didichuxing.horoscope.runtime.Value
import com.didichuxing.horoscope.util.Utils.configIntOrDefault
import com.didichuxing.horoscope.util.{Logging, SystemLog}
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.{KafkaConsumer, _}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
 * kakfa数据源，默认将数据统一转换为List[Value]，支持多线程拉取
 *
 * @param sourceConfig 配置
 * @param builder      源数据构造函数
 */
class KafkaSource[K, V](sourceConfig: Config, builder: EventBuilder[List[ConsumerRecord[K, V]], List[Value]])
  extends Source with Logging {

  case class ConsumerControl(status: AtomicBoolean, consumer: KafkaConsumer[K, V], complete: Future[Unit])

  var eventBus: EventBus = _
  val controls = ListBuffer[ConsumerControl]()
  val sourceName = sourceConfig.getString("source-name")
  val params = sourceConfig.getConfig("parameter")
  val kafkaConcurrency = configIntOrDefault(params, "kafka.concurrency", 1)
  val maxThreadPoolSize = if (kafkaConcurrency == 0) 1 else kafkaConcurrency
  val pullThreadPool = TtlExecutors.getTtlExecutorService(new ThreadPoolExecutor(kafkaConcurrency,
    maxThreadPoolSize, 1000L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue[Runnable](10),
    new NamedThreadFactory(s"kafka-$sourceName")))

  override def start(eventBus: EventBus): Unit = {
    eventBus.start()
    this.eventBus = eventBus
    for (i <- 1 to kafkaConcurrency) {
      val status = new AtomicBoolean(false)
      val props = kafkaConfig
      val consumer = new KafkaConsumer[K, V](props)
      val complete = pullThreadPool.submit(new KafkaPoll(eventBus, consumer, status))
      val cc = ConsumerControl(status, consumer, complete)
      controls.append(cc)
      info(("msg", s"start pull"), ("source", sourceName), ("thread", i), ("props", props))
    }
  }

  override def stop(): Unit = {
    for (cc <- controls) {
      if (cc.status.compareAndSet(true, false)) {
        cc.consumer.wakeup()
        Try(cc.complete.get(10, TimeUnit.SECONDS))
      }
    }
    if (eventBus != null) {
      eventBus.stop()
    }
    pullThreadPool.shutdown()
  }

  private val kafkaConfig: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", params.getString("kafka.servers"))
    props.put("group.id", params.getString("kafka.group"))
    props.put("auto.offset.reset", "latest")
    props.put("enable.auto.commit", "false")
    props.put("auto.commit.interval.ms", "0")
    //1s
    props.put("retry.backoff.ms", "1000")
    //default 20 min
    props.put("request.timeout.ms", Try(params.getString("kafka.request.timeout")).getOrElse("1200000"))
    //default 5 min
    props.put("session.timeout.ms", Try(params.getString("kafka.session.timeout")).getOrElse("300000"))
    //default 15 min
    props.put("max.poll.interval.ms", Try(params.getString("kafka.poll.interval")).getOrElse("900000"))
    props.put("key.deserializer", Try(params.getString("kafka.key.deserializer")).getOrElse(
      "org.apache.kafka.common.serialization.StringDeserializer"))
    props.put("value.deserializer", Try(params.getString("kafka.value.deserializer")).getOrElse(
      "org.apache.kafka.common.serialization.StringDeserializer"))
    props.put("max.poll.records", Try(params.getString("kafka.max")).getOrElse("1"))
    //didi certification
    props.put("security.protocol", "SASL_PLAINTEXT")
    props.put("sasl.mechanism", "PLAIN")
    //请填写正确的clusterId，appId，密码//请填写正确的clusterId，appId，密码
    val clusterId = params.getString("kafka.cluster-id")
    val appId = params.getString("kafka.app-id")
    val password = params.getString("kafka.password")
    val securityText = "org.apache.kafka.common.security.plain.PlainLoginModule required"
    //请填写正确的clusterId，appId，密码
    val format = "%s username=\"%s.%s\" password=\"%s\";"
    val jaas_config = String.format(format, {securityText}, {clusterId}, {appId}, {password})
    props.put("sasl.jaas.config", jaas_config)
    props
  }

  class KafkaPoll(eventBus: EventBus, consumer: KafkaConsumer[K, V], status: AtomicBoolean) extends Callable[Unit] {

    override def call(): Unit = {
      if (status.compareAndSet(false, true)) {
        try {
          val topic = params.getString("kafka.topic")
          val topics = topic.split(",").map(v => v.trim)
          consumer.subscribe(util.Arrays.asList[String](topics: _*))
          info(("msg", "kafka begin pulling"), ("source", sourceName), ("topic", topic))
          while (status.get()) {
            val records = consumer.poll(100)
            if (records.count() > 0) {
              process(records)
            }
          }
        } catch {
          case ex: Throwable =>
            ex.printStackTrace()
            error(("msg", "kafka consumer error"), ("ex", ex.getCause))
        } finally {
          consumer.close()
          info(("msg", "kafka source close"), ("source", sourceName))
        }
      } else {
        error("kafka consumer has being started")
      }
    }

    private def process(records: ConsumerRecords[K, V]): Unit = {
      val beginTime = System.currentTimeMillis()
      try {
        SystemLog.create()
        info(("msg", "receive message"), ("source", sourceName), ("count", records.size))
        //转换为list
        val recordList = records.iterator().toList
        //构造为soureEvents
        val events = builder(recordList)
        //成功消费的消息
        val successEvents = eventBus.process(events)
        //计算提交的offset
        val commitOffsets = commitOffset(recordList, successEvents)
        if (commitOffsets.size == 0) {
          //全部回滚
          val rollbackOffsets = rollbackOffset(records)
          rollbackOffsets.foreach(tpOffset => {
            consumer.seek(tpOffset._1, tpOffset._2.offset())
            error(("kafka rollback", s"${tpOffset._1}-${tpOffset._2.offset()}"))
          })
        } else {
          //提交成功的offset
          commitOffsets.foreach(tpOffset => {
            consumer.seek(tpOffset._1, tpOffset._2.offset())
            debug(("kafka commit", s"${tpOffset._1}-${tpOffset._2.offset()}"))
          })
          commitAsync(consumer, commitOffsets)
        }
      } catch {
        case ex: Exception =>
          ex.printStackTrace()
          error(("msg", "kafka pull message error"), ("ex", ex.getMessage))
          //全部回滚
          val rollbackOffsets = rollbackOffset(records)
          rollbackOffsets.foreach(tpOffset => {
            consumer.seek(tpOffset._1, tpOffset._2.offset())
            error(("kafka rollback", s"${tpOffset._1}-${tpOffset._2.offset()}"))
          })
      } finally {
        val endTime = System.currentTimeMillis()
        info(("msg", "total time"), ("source", sourceName), ("proc_time", s"${endTime - beginTime}ms"))
      }
    }

    /**
     * 异步提交offset，如果有异常会记录日志
     *
     * @param consumer
     * @param commitOffsets
     */
    private def commitAsync(consumer: KafkaConsumer[K, V],
                            commitOffsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
      consumer.commitAsync(commitOffsets, new OffsetCommitCallback() {
        override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata],
                                exception: Exception): Unit = {
          if(exception != null) {
            exception.printStackTrace()
            exception match {
              case ex: RetriableCommitFailedException =>
                //https://issues.apache.org/jira/browse/KAFKA-5052
                warn(("async commit retry", offsets), ("ex", ex.getMessage))
                //异步提交重试异常，暂时改成以同步的方式重试一次
                try {
                  consumer.commitSync(offsets)
                } catch {
                  case ex: Throwable =>
                    //ex.printStackTrace()
                    warn(("retry sync commit error", offsets), ("ex", ex.getMessage))
                }
              case ex: Exception =>
                warn(("async commit error", offsets), ("ex", ex.getMessage))
            }
          }
        }
      })
    }

    /**
     * 计算本次成功的offset，类似TCP的滑动窗口，每次批量发送多个消息，每个partition成功的最后一个offset提交
     *
     * @param recordList
     * @param successEvents
     * @return
     */
    private def commitOffset(recordList: List[ConsumerRecord[K, V]],
                             successEvents: List[FlowEvent]): Map[TopicPartition, OffsetAndMetadata] = {
      var successCount = 0
      var failCount = 0
      val result = mutable.Map[TopicPartition, OffsetAndMetadata]()
      //先遍历正常提交的
      for ((event, i) <- successEvents.zipWithIndex) {
        val record = recordList.get(i)
        val currOffset = record.offset()
        val tp = new TopicPartition(record.topic(), record.partition())
        if (result.get(tp).isEmpty) {
          result.put(tp, new OffsetAndMetadata(currOffset))
        }
        val commitOffset = result.get(tp).get.offset()
        if (event != null) {
          successCount += 1
          //正常返回结果的，找到该partition中最大的offset+1
          val nextOffset = currOffset + 1
          if (nextOffset > commitOffset) {
            result.put(tp, new OffsetAndMetadata(nextOffset))
          }
        }
      }
      //再遍历需要回滚的
      for ((event, i) <- successEvents.zipWithIndex) {
        val record = recordList.get(i)
        val currOffset = record.offset()
        val tp = new TopicPartition(record.topic(), record.partition())
        if (result.get(tp).isEmpty) {
          result.put(tp, new OffsetAndMetadata(currOffset))
        }
        val commitOffset = result.get(tp).get.offset()
        if (event == null) {
          failCount += 1
          //未返回结果的，找到该partition中最小的offset
          if (currOffset < commitOffset) {
            result.put(tp, new OffsetAndMetadata(currOffset))
          }
        }
      }
      info(("msg", "commit events"), ("source", sourceName), ("successCount", successCount),
        ("failCount", failCount))
      result.toMap
    }

    /**
     * 全部回滚
     *
     * @param records
     * @return
     */
    private def rollbackOffset(records: ConsumerRecords[K, V]): Map[TopicPartition, OffsetAndMetadata] = {
      val result = mutable.Map[TopicPartition, OffsetAndMetadata]()
      for (par <- records.partitions()) {
        val parRecords = records.records(par)
        for (record <- parRecords) {
          val tp = new TopicPartition(record.topic(), record.partition())
          val offset = record.offset()
          val currOffset = result.getOrElse(tp, new OffsetAndMetadata(0))
          if (currOffset.offset() == 0 || offset < currOffset.offset()) {
            result.put(tp, new OffsetAndMetadata(offset))
          }
        }
      }
      result.toMap
    }
  }

}
