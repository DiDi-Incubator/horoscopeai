package com.didichuxing.horoscope.core

import akka.http.scaladsl.server.Route
import com.typesafe.config.Config

trait ClusterStore {

  //获取集群列表
  def getClusters(): Seq[Config]

  //获取集群当前服务器
  def getHosts(): Seq[String]

  def api: Route = _.reject()

}
