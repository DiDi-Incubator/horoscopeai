/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.builtin

import java.time.{Instant, LocalDateTime, ZoneId}

import com.didichuxing.horoscope.runtime.expression.BuiltIn
import com.didichuxing.horoscope.util.{DateUtil, Logging}

object DateBuiltIn extends Logging {

  implicit val builtin: BuiltIn = new BuiltIn.Builder()
    .addFunction("timestamp")(timestamp _)
    .addFunction("parse_time")(parseTime _)
    .addMethod("hour")(hour)
    .build()

  def timestamp(): Long = {
    System.currentTimeMillis()
  }

  def parseTime(s: String): Long = {
    DateUtil.dateFormatOrigin.get().parse(s).getTime
  }

  def hour(timestamp: Long)(): Long = {
    val instant = Instant.ofEpochSecond(timestamp)
    LocalDateTime.ofInstant(instant, ZoneId.systemDefault()).getHour
  }

}
