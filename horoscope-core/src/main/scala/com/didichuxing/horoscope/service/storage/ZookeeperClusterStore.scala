package com.didichuxing.horoscope.service.storage

import akka.http.scaladsl.server.Directives.get
import akka.http.scaladsl.server.Route
import com.didichuxing.horoscope.core.ClusterStore
import com.didichuxing.horoscope.service.resource.{ResourceManager, ZkClient}
import com.didichuxing.horoscope.util.{Logging, SystemLog}
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}

import scala.util.Try

/**
 * namespace
 * | -- cluster1(config)
 * | -- cluster(leader select)
 * | -- host1
 * | -- host2
 * | -- ...
 * | -- flows
 * | -- demo
 * | -- demo.flow
 * | -- ...
 * | -- ...
 * | -- sources
 * | -- source-name(config)
 * | -- ...
 * | -- cluster2
 */
class ZookeeperClusterStore(zkClient: ZkClient, rm: ResourceManager) extends ClusterStore with Logging {

  override def getClusters(): Seq[Config] = {
    zkClient.getChild("/").map(cluster => {
      val configOpt = zkClient.getData(s"/$cluster")
      if (configOpt.isDefined) {
        Try(ConfigFactory.parseString(configOpt.get)).getOrElse(ConfigFactory.empty())
      } else {
        ConfigFactory.empty()
      }
    }).filter(c => !c.isEmpty)
  }

  override def getHosts(): Seq[String] = {
    rm.getParticipants().map(p => p.getHost())
  }

  override def api: Route = {
    import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
    import akka.http.scaladsl.server.Directives._
    import spray.json._
    import DefaultJsonProtocol._
    concat(
      get {
        //获取某个集群下的服务器信息
        path("hosts") {
          SystemLog.create()
          complete(getHosts.toJson)
        }
      },
      get {
        //获取集群列表
        SystemLog.create()
        val clusters = getClusters().map(config => config.root().render(ConfigRenderOptions.concise()).parseJson)
        complete(clusters.toJson)
      }
    )
  }
}

object ZookeeperClusterStore {
  def apply(zkClient: ZkClient, rm: ResourceManager): ZookeeperClusterStore = new ZookeeperClusterStore(zkClient, rm)
}
