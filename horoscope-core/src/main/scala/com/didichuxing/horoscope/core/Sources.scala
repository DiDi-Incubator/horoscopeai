/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.core

import com.didichuxing.horoscope.runtime.Value
import com.didichuxing.horoscope.service.source._
import org.apache.kafka.clients.consumer.ConsumerRecord

object Sources {

  type EventBuilder[T, E] = T => E // E must contains "trace" field

  def kafka[K, V](builder: EventBuilder[List[ConsumerRecord[K, V]], List[Value]]): SourceFactory = {
    new KafkaSourceFactory[K, V](builder)
  }

  def scheduler(builder: EventBuilder[List[Array[Byte]], List[Value]]): SourceFactory = {
    new SchedulerSourceFactory(builder)
  }

  def http[T](builder: EventBuilder[T, Value]): SourceFactory = {
    new PushSourceFactory(builder)
  }
}
