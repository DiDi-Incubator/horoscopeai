/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import java.util.concurrent._

import com.alibaba.ttl.threadpool.TtlExecutors
import com.didichuxing.horoscope.util.Utils
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

/**
 * All source executions use the same thread pool. include rpc callback, flow executor callback, grpc executor
 * The thread pool size is not limited, using a synchronous queue,
 * the thread survives for 1 minute if no message is retrieved
 */
trait SourceExecutionContext {
  def start()

  def stop()

  def getExecutionContext(): ExecutionContextExecutorService
}

class DefaultSourceExecutionContext(config: Config) extends SourceExecutionContext {

  var ec: ExecutionContextExecutorService = _

  def start(): Unit = {
    val max = Utils.configIntOrDefault(config, "horoscope.max-thread", Integer.MAX_VALUE)
    val tf = new NamedThreadFactory("horoscope")
    val pool = new ThreadPoolExecutor(0, max, 60L, TimeUnit.SECONDS, new SynchronousQueue[Runnable], tf)
    //val pool = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(), max,
    //  60L, TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable](2000), tf)
    val ttlPool = TtlExecutors.getTtlExecutorService(pool)
    ec = ExecutionContext.fromExecutorService(ttlPool)
  }

  def stop(): Unit = {
    if (ec != null) {
      ec.shutdown()
    }
  }

  def getExecutionContext(): ExecutionContextExecutorService = ec

}

object DefaultSourceExecutionContext {
  def apply(config: Config): DefaultSourceExecutionContext = {
    val sec = new DefaultSourceExecutionContext(config)
    sec.start()
    sec
  }
}
