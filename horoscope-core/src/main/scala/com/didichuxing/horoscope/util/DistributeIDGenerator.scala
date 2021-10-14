package com.didichuxing.horoscope.util

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode

// 基于zk的自增id生成器
class DistributeIDGenerator(counterPath: String, curator: CuratorFramework) extends Logging {
  private val retryPolicy = new ExponentialBackoffRetry(100, 5)
  private val distAtomicLong = new DistributedAtomicLong(curator, counterPath, retryPolicy)

  init()

  def init(): Unit = {
    val stat = curator.checkExists().forPath(counterPath)
    if (stat == null) {
      curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(counterPath)
      distAtomicLong.forceSet(0)
    }
  }

  def nextId(): Long = {
    try {
      val sequence = distAtomicLong.increment()
      if (sequence.succeeded()) {
        sequence.postValue()
      } else {
        throw new RuntimeException("generate id failed")
      }
    } catch {
      case e: Throwable =>
        throw new RuntimeException(s"generate id with exception: ${e.getMessage}")
    }
  }
}
