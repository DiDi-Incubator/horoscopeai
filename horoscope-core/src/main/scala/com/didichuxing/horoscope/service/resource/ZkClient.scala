/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.resource

import java.nio.charset.StandardCharsets

import com.didichuxing.horoscope.util.Constants._
import com.typesafe.config.{Config, ConfigRenderOptions}
import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheListener}
import org.apache.curator.framework.recipes.leader.LeaderLatch
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryForever
import org.apache.zookeeper.CreateMode

import scala.collection.JavaConversions._
import scala.util.Try

class ZkClient(config: Config) {

  val servers = config.getString("horoscope.zookeeper.servers")
  val namespace = Try(config.getString("horoscope.zookeeper.namespace")).getOrElse("horoscope6")
  val clusterConfig = config.getConfig("horoscope.zookeeper.cluster")
  val clusterPath = s"/${Try(clusterConfig.getString("name")).getOrElse("")}"
  val curator = CuratorFrameworkFactory.builder()
    .connectString(servers)
    .namespace(namespace)
    .sessionTimeoutMs(40000)
    .connectionTimeoutMs(40000)
    .retryPolicy(new RetryForever(5000))
    .build()
  curator.start()
  create(clusterPath)
  setData(clusterPath, clusterConfig)

  /**
   * 停止服务
   */
  def stop(): Unit = {
    assert(curator != null, "curator is null")
    curator.close()
  }

  def getSourcePath(): String = {
    s"$clusterPath/$ZK_SOURCE_PATH"
  }

  def getClusterPath(): String = {
    s"$clusterPath/$ZK_CLUSTER_PATH"
  }

  def getConfigPath(): String = {
    s"$clusterPath/$ZK_CONF_PATH"
  }

  def flowsCurator(): CuratorFramework = {
    curator.usingNamespace(s"$namespace$clusterPath/$ZK_FLOW_PATH")
  }

  def udfCurator(): CuratorFramework = {
    curator.usingNamespace(s"$namespace$clusterPath/$ZK_UDF_PATH")
  }

  def configCurator(): CuratorFramework = {
    curator.usingNamespace(namespace)
  }

  def exist(path: String): Boolean = {
    assert(curator != null, "curator is null")
    val stat = curator.checkExists().forPath(path)
    if (stat == null) {
      false
    } else {
      true
    }
  }

  /**
   * 创建一个新节点，如果父节点不存在，递归创建
   *
   * @param path
   * @param model
   */
  def create(path: String, model: CreateMode): Boolean = {
    assert(curator != null, "curator is null")
    if (!exist(path)) {
      curator.create().creatingParentsIfNeeded().withMode(model).forPath(path)
      true
    } else {
      false
    }
  }

  def create(path: String): Boolean = {
    create(path, CreateMode.PERSISTENT)
  }

  def setData(path: String, json: String): Boolean = {
    assert(curator != null, "curator is null")
    if (exist(path)) {
      curator.setData().forPath(path, json.getBytes(StandardCharsets.UTF_8))
      true
    } else {
      false
    }
  }

  def setData(path: String, config: Config): Boolean = {
    assert(curator != null, "curator is null")
    if (exist(path)) {
      val configStr = config.root().render(ConfigRenderOptions.concise())
      curator.setData().forPath(path, configStr.getBytes(StandardCharsets.UTF_8))
      true
    } else {
      false
    }
  }

  def getData(path: String): Option[String] = {
    assert(curator != null, "curator is null")
    if (exist(path)) {
      val data = new String(curator.getData().forPath(path), StandardCharsets.UTF_8)
      Some(data)
    } else {
      None
    }
  }

  /**
   * 删除节点
   * @param path
   */
  def delete(path: String): Unit = {
    assert(curator != null, "curator is null")
    if (exist(path)) {
      curator.delete().forPath(path)
    }
  }

  /**
   * 获取子目录
   * @param path
   * @return
   */
  def getChild(path: String): List[String] = {
    assert(curator != null, "curator is null")
    curator.getChildren().forPath(path).toList
  }

  def leaderLatch(path: String, participantId: String): LeaderLatch = {
    val ll = new LeaderLatch(curator, path, participantId)
    ll.start()
    ll
  }

  def watchNode(path: String, listener: PathChildrenCacheListener): PathChildrenCache = {
    val childrenCache = new PathChildrenCache(curator, path, true)
    childrenCache.start()
    childrenCache.getListenable().addListener(listener)
    childrenCache
  }
}
