/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 */

package com.didichuxing.horoscope.util

import java.util.Date
import java.text.DateFormat
import java.text.SimpleDateFormat

import com.typesafe.config.Config

class PublicLog(key: String) extends Logging {

  val safeSdf: ThreadLocal[DateFormat] = new ThreadLocal[DateFormat]() {
    override protected def initialValue = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  }

  def public(message: (String, Any)*): Unit = {
    logging.info(logInfo(key, SystemLog.get(),
      ("timestamp", safeSdf.get().format(new Date)) +: message))
  }

}

object PublicLog {
  def apply(config: Config): PublicLog = {
    val key = if (config.hasPath("horoscope.public-log-key")) {
      config.getString("horoscope.public-log-key")
    } else {
      "horoscope"
    }
    new PublicLog(key)
  }
}
