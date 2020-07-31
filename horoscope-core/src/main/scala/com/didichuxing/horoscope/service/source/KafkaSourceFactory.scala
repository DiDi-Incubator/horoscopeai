/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import com.didichuxing.horoscope.core.Sources.EventBuilder
import com.didichuxing.horoscope.core.{Source, SourceFactory}
import com.didichuxing.horoscope.runtime.Value
import com.didichuxing.horoscope.util.PBParserUtil
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.ConsumerRecord

class KafkaSourceFactory[K, V](builder: EventBuilder[List[ConsumerRecord[K, V]], List[Value]]) extends SourceFactory {

  override def newSource(config: Config): Source = {
    new KafkaSource(config, builder)
  }
}

class KafkaPBSourceFactory[K, V](sourcePBEventBuilder: String => EventBuilder[List[ConsumerRecord[K, V]], List[Value]]
) extends SourceFactory {
  override def newSource(config: Config): Source = {
    val className = config.getString("class-name")
    if (PBParserUtil.containsMessageType(className)) {
      new KafkaSource(config, sourcePBEventBuilder(className))
    } else {
      throw new IllegalAccessException(s"Class name: ${className} doesn't exist")
    }
  }
}
