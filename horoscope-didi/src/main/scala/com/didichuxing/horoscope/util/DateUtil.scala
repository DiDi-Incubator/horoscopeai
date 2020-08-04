/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.util

import java.text.SimpleDateFormat
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import java.util.{Calendar, Date}

object DateUtil {
  def getDateFormat(pattern: String): ThreadLocal[SimpleDateFormat] = new ThreadLocal[SimpleDateFormat]() {
    override def initialValue() = {
      new SimpleDateFormat(pattern)
    }
  }

  val dateFormat: ThreadLocal[SimpleDateFormat] = getDateFormat("yyyy-MM-dd'T'HH:mm:ss")

  val dateFormatOrigin: ThreadLocal[SimpleDateFormat] = getDateFormat("yyyy-MM-dd HH:mm:ss")

  val startFormat: ThreadLocal[SimpleDateFormat] = getDateFormat("yyyy-MM-dd'T'00:00:00")

  val endFormat: ThreadLocal[SimpleDateFormat] = getDateFormat("yyyy-MM-dd'T'23:59:59")

  val mapVersionFormat: ThreadLocal[SimpleDateFormat] = getDateFormat("yyyyMMddHH")

  def mapVersionTimestamp(mapVersion: String): Long = {
    mapVersionFormat.get().parse(mapVersion).getTime
  }

  def getTodayEnd(): Long = {
    val calendar = Calendar.getInstance()
    calendar.set(Calendar.HOUR_OF_DAY, 23)
    calendar.set(Calendar.MINUTE, 59)
    calendar.set(Calendar.SECOND, 59)
    calendar.getTimeInMillis
  }

  def getTodayStart(): Long = {
    val calendar = Calendar.getInstance()
    calendar.set(Calendar.HOUR_OF_DAY, 0)
    calendar.set(Calendar.MINUTE, 0)
    calendar.set(Calendar.SECOND, 0)
    calendar.getTimeInMillis
  }

  def getOneDayStart(timeStamp: Long): Long = {
    val timeString = startFormat.get().format(timeStamp)
    startFormat.get().parse(timeString).getTime()
  }

  def getOneDayEnd(timeStamp: Long): Long = {
    val timeString = endFormat.get().format(timeStamp)
    dateFormat.get().parse(timeString).getTime()
  }

  def formatTimestamp(timestamp: Long): String = {
    val date = new Date
    date.setTime(timestamp)
    dateFormat.get().format(date)
  }

  def formatLocalDateTime(localDateTime: LocalDateTime): String = {
    formatTimestamp(toEpochMilli(localDateTime))
  }

  def formatLocalDate(localDate: LocalDate): String = {
    formatTimestamp(toEpochMilli(localDate))
  }

  def formatAsDate(timestamp: Long): String = {
    Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).toLocalDate.toString
  }

  def toLocalDate(timestamp: Long): LocalDate = {
    Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).toLocalDate
  }

  def toLocalDateTime(timestamp: Long): LocalDateTime = {
    Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).toLocalDateTime
  }

  def getTimestamp(dateStr: String): Long = {
    dateFormat.get().parse(dateStr).getTime
  }

  def getVersionTimestamp(dateStr: String): Long = {
    mapVersionFormat.get().parse(dateStr).getTime
  }

  def toEpochMilli(dateTime: LocalDateTime): Long = dateTime.atZone(ZoneId.systemDefault()).toInstant.toEpochMilli

  def toEpochMilli(localDate: LocalDate): Long =
    localDate.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant.toEpochMilli

  def yesterday(): LocalDate = {
    LocalDate.now().minusDays(1)
  }
}
