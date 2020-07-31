/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

class NamedThreadFactory(source: String) extends ThreadFactory {

  private var group: ThreadGroup = _
  private val threadNumber = new AtomicInteger(1)
  private var namePrefix: String = null

  start()

  def start(): Unit = {
    val s = System.getSecurityManager
    if (s != null) {
      group = s.getThreadGroup
    } else {
      group = Thread.currentThread.getThreadGroup
    }
    namePrefix = source + "-pool-thread-"
  }

  override def newThread(r: Runnable): Thread = {
    val t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement, 0)
    if (t.isDaemon) t.setDaemon(false)
    if (t.getPriority != Thread.NORM_PRIORITY) t.setPriority(Thread.NORM_PRIORITY)
    t
  }
}
