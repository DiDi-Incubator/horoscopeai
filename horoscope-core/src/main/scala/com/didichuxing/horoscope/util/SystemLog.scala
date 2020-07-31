/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.util

import java.util.UUID

import com.alibaba.ttl.TransmittableThreadLocal

/**
 * event trace: Horoscope based on trace serial execution of events
 * system trace: Trace trace of the horoscope distributed call
 */
object SystemLog {

  //声明一个线程本地trace context，key就是sysTraceInfo实例，值是String
  private val sysLogInfo = new TransmittableThreadLocal[String]()

  def create(): Unit = {
    sysLogInfo.remove()
    val logId = UUID.randomUUID().toString.replace("-", "")
    sysLogInfo.set(logId)
  }

  def set(logId: String): Unit = {
    sysLogInfo.set(logId)
  }

  def get(): String = {
    if (sysLogInfo.get() == null) {
      create()
    }
    sysLogInfo.get()
  }

}
