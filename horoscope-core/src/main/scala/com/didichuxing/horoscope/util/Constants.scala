/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.util

object Constants {

  /**
   * 数据归属slot总量，每个机器负责一定数量的slot
   * 16384参考了redis的设计，2的14次方
   */
  val CLUSTER_SLOT_COUNT = 16384

  /**
   * trace上下文变量标志位
   */
  val CONTEXT_VAL_FLAG = "$"

  /**
   * json格式消息，traceId字段
   */
  val TRACE_ID = "traceId"

  /**
   * json格式消息，eventId字段
   */
  val EVENT_ID = "eventId"

  /**
   * scheduler的内部source源工厂类
   */
  val SCH_SOURCE_FACTORY = "schedulerSourceFactory"

  /**
   * scheduler 延时执行数据源
   */
  val SCH_DELAY_SOURCE = "delaySource"

  /**
   * scheduler 立即执行数据源
   */
  val SCH_IMME_SOURCE = "immediatelySource"

  /**
   * rowkey多个字段的间隔符
   */
  val ROWKEY_MARK =  "_"

  /**
   * sourcekey分割符
   */
  val SOURCEKEY_MARK =  "-"

  /**
   * 持续监控recovery每次批量获取的消息数量
   */
  val SCH_BATCH_SIZE = 2048

  /**
   * zk存储泛源配置路径
   */
  val ZK_SOURCE_PATH = "sources"
  /**
   * zk存储flow路径
   */
  val ZK_FLOW_PATH = "flows"
  /**
   * zk集群选主路径
   */
  val ZK_CLUSTER_PATH = "cluster"
}
