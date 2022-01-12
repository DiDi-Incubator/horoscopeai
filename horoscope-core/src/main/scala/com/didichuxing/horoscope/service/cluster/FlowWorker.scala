/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.cluster

import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.util.Logging

import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * 集群版本的服务启动
 */
class FlowWorker(implicit ctx: ApplicationContext) extends Logging {

  implicit val config = ctx.config
  implicit val sourceFactories = ctx.sourceFactories
  implicit val resourceManager = ctx.resourceManager
  implicit val flowExecutor = ctx.flowExecutor
  implicit val zkClient = ctx.zkClient
  implicit val system = ctx.system
  implicit val sec = ctx.sourceExecutionContext
  /**
   * 启动顺序
   * actor->flowExecutor->rocksDB->schedulerSource->zk->leaderSelector->recoveryScheduler
   */
  def startService(): Unit = {
    flowExecutor.start()
    resourceManager.start()
    info("All service started")
  }

  /**
   * 关闭顺序
   * leaderSelector->schedulerSource->rocksDB->flowExecutor->actor->zk
   */
  def stopService(): Unit = {
    resourceManager.stop()
    flowExecutor.stop()
    Await.result(system.terminate(), 10 seconds)
    zkClient.stop()
    sec.stop()
    info("All service stopped")
  }

}
