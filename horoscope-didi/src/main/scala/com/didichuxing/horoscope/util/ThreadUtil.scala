/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.util

import java.util.concurrent.{ExecutorService, Executors, ScheduledExecutorService, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

object ThreadUtil extends Logging {

  def createThreadPool(name: String,
                       size: Int = 0,
                       isDaemon: Boolean = true): ExecutionContextExecutorService = {
    val threadFactory = new ThreadFactoryBuilder()
      .setDaemon(isDaemon)
      .setNameFormat(s"$name-thread-%d")
      .build()
    val executors = if (size == 0) {
      Executors.newCachedThreadPool(threadFactory)
    } else if (size == 1) {
      Executors.newSingleThreadExecutor(threadFactory)
    } else {
      Executors.newFixedThreadPool(size, threadFactory)
    }
    ExecutionContext.fromExecutorService(executors)
  }

  def createScheduledThreadPool(name: String,
                                size: Int = 0,
                                isDaemon: Boolean = true): ScheduledExecutorService = {
    val threadFactory = new ThreadFactoryBuilder()
      .setDaemon(isDaemon)
      .setNameFormat(s"$name-thread-%d")
      .build()
    val executors = Executors.newScheduledThreadPool(size, threadFactory)
    executors
  }

  def shutdownThreadPool(executor: ExecutorService,
    duration: Duration = Duration.create(10, TimeUnit.SECONDS)): Unit = {
    executor.shutdown()
    try {
      executor.awaitTermination(duration.toMillis, TimeUnit.MILLISECONDS)
    } finally {
      if (!executor.isShutdown) {
        executor.shutdownNow()
      }
    }
  }

}
