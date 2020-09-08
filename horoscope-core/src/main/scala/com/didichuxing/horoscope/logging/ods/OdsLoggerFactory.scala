/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: liangguorong@didiglobal.com
 */

package com.didichuxing.horoscope.logging.ods

import com.didichuxing.horoscope.core.OdsLogger
import com.didichuxing.horoscope.logging.ods.impl.{CompositeOdsLogger, KafkaOdsLogger, LocalOdsLogger}
import com.didichuxing.horoscope.util.PublicLog
import com.typesafe.config.Config

object OdsLoggerFactory {

  def newLogger(config: Config): OdsLogger = {
    if (config.getBoolean("horoscope.ods-logger.local-enabled")) {
      new CompositeOdsLogger(Seq(newLocalLogger(config), new KafkaOdsLogger(config)))
    } else {
      newKafkaLogger(config)
    }
  }

  def newLocalLogger(config: Config): OdsLogger = new LocalOdsLogger(PublicLog(config))

  def newKafkaLogger(config: Config): OdsLogger = new KafkaOdsLogger(config)

}
