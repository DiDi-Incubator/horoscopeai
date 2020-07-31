/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.resource

import java.lang.reflect.Type
import java.nio.charset.StandardCharsets

import com.google.gson.GsonBuilder
import com.typesafe.config.Config
import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheListener}
import org.apache.curator.framework.recipes.leader.LeaderLatch
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryForever
import org.apache.zookeeper.CreateMode

import scala.collection.JavaConversions._

class ZkClient {

  var curator: CuratorFramework = _
  val gson = new GsonBuilder().create()

  /**
   * 启动服务
   */
  def start(config: Config): Unit = {
    val servers = config.getString("horoscope.zookeeper.servers")
    val ns = config.getString("horoscope.zookeeper.namespace")
    curator = CuratorFrameworkFactory.builder()
      .connectString(servers)
      .namespace(ns)
      .sessionTimeoutMs(40000)
      .connectionTimeoutMs(40000)
      .retryPolicy(new RetryForever(5000))
      .build()
    curator.start()
  }

  /**
   * 停止服务
   */
  def stop(): Unit = {
    assert(curator != null, "curator is null")
    curator.close()
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

  /**
   * 将对象转换为json传，放入节点
   *
   * @param path
   * @param data
   */
  def setData[T](path: String, data: T, ct: Type): Boolean = {
    assert(curator != null, "curator is null")
    if (exist(path)) {
      val json = gson.toJson(data)
      curator.setData().forPath(path, json.getBytes(StandardCharsets.UTF_8))
      true
    } else {
      false
    }
  }

  /**
   * 获取zk节点数据
   * @param path
   * @param ct
   * @tparam T
   * @return
   */
  def getData[T](path: String, ct: Type): Option[T] = {
    assert(curator != null, "curator is null")
    if (exist(path)) {
      val data = new String(curator.getData().forPath(path))
      Some(gson.fromJson(data, ct))
    } else {
      None
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
