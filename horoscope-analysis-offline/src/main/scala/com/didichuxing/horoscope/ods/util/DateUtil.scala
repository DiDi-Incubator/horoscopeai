/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: liangguorong@didiglobal.com
 */

package com.didichuxing.horoscope.ods.util

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import scala.collection.mutable.ArrayBuffer

object DateUtil {

  val YEAR = DateTimeFormatter.ofPattern("yyyy")
  val MONTH = DateTimeFormatter.ofPattern("MM")
  val DAY = DateTimeFormatter.ofPattern("dd")
  val HOUR = DateTimeFormatter.ofPattern("HH")
  val DATE_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")

  /**
   * @param t          "2020-07-05 11:00"
   * @param offsetHour -2 means "2020-07-05 11:00" - 2 hour
   * @return Array[("2020", "07", "05", "11"), ...]
   */
  def dateRange(t: String, offsetHour: Int): Seq[(String, String, String, String)] = {
    var startTime: LocalDateTime = null
    var endTime: LocalDateTime = null
    if (offsetHour > 0) {
      startTime = LocalDateTime.from(DATE_TIME.parse(t))
      endTime = startTime.plusHours(offsetHour)
    } else {
      endTime = LocalDateTime.from(DATE_TIME.parse(t))
      startTime = endTime.minusHours(Math.abs(offsetHour))
    }
    val buffer = new ArrayBuffer[(String, String, String, String)]
    while (startTime.isBefore(endTime) || startTime.isEqual(endTime)) {
      buffer += ((
        startTime.format(YEAR),
        startTime.format(MONTH),
        startTime.format(DAY),
        startTime.format(HOUR)
      ))
      startTime = startTime.plusHours(1)
    }
    buffer
  }

}
