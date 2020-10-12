/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.builtin

import java.time.{Duration, Instant, LocalDateTime, ZoneId}
import java.util.Calendar

import com.didichuxing.horoscope.runtime.expression.BuiltIn
import com.didichuxing.horoscope.util.{DateUtil, Logging}

object DateBuiltIn extends Logging {

  implicit val builtin: BuiltIn = new BuiltIn.Builder()
    .addFunction("timestamp")(timestamp _)
    .addFunction("parse_time")(parseTime _)
    .addMethod("hour")(hour)
    .addFunction("hour_ceil")(hourCeil _)
    .addFunction("hour_floor")(hourFloor _)
    .addFunction("day_ceil")(dayCeil _)
    .addFunction("day_floor")(dayFloor _)
    .addFunction("ten_minutes_ceil")(tenMinutesCeil _)
    .addFunction("ten_minutes_floor")(tenMinutesFloor _)
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

  def dayFloor(t: Long): Long = {
    val zoneOffsetInSeconds = Calendar.getInstance().get(Calendar.ZONE_OFFSET) / 1000
    Math.floor((t + zoneOffsetInSeconds).toDouble / ONE_DAY_IN_SECOND).toLong * ONE_DAY_IN_SECOND - zoneOffsetInSeconds
  }

  def dayCeil(t: Long): Long = {
    val zoneOffsetInSeconds = Calendar.getInstance().get(Calendar.ZONE_OFFSET) / 1000
    Math.ceil((t + zoneOffsetInSeconds).toDouble / ONE_DAY_IN_SECOND).toLong * ONE_DAY_IN_SECOND - zoneOffsetInSeconds
  }

  def hourFloor(t: Long): Long = {
    Math.floor(t.toDouble / ONE_HOUR_IN_SECOND).toLong * ONE_HOUR_IN_SECOND
  }

  def hourCeil(t: Long): Long = {
    Math.ceil(t.toDouble / ONE_HOUR_IN_SECOND).toLong * ONE_HOUR_IN_SECOND
  }

  def getHour(t: Long): Long = {
    val zoneOffsetInSeconds = Calendar.getInstance().get(Calendar.ZONE_OFFSET) / 1000
    (t + zoneOffsetInSeconds) / ONE_HOUR_IN_SECOND % 24
  }

  def tenMinutesFloor(t: Long): Long = {
    Math.floor(t.toDouble / TEN_MINUTES_IN_SECOND).toLong * TEN_MINUTES_IN_SECOND
  }

  def tenMinutesCeil(t: Long): Long = {
    Math.ceil(t.toDouble / TEN_MINUTES_IN_SECOND).toLong * TEN_MINUTES_IN_SECOND
  }

  val TEN_MINUTES_IN_SECOND = Duration.ofMinutes(10).getSeconds
  val ONE_HOUR_IN_SECOND = Duration.ofHours(1).getSeconds
  val ONE_DAY_IN_SECOND = Duration.ofDays(1).getSeconds
}
