package com.didichuxing.horoscope.service.storage

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.{Route, StandardRoute}
import com.didichuxing.horoscope.core.{ConfigChangeListener, ConfigChecker, ConfigStore}
import com.didichuxing.horoscope.util.{DistributeIDGenerator, FlowConfParser, Logging, Utils}
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions, ConfigValue, ConfigValueFactory}
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache._
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.data.Stat
import spray.json.{JsValue, _}
import java.util.concurrent.{Executors, Semaphore}

import com.didichuxing.horoscope.service.resource.ZkClient
import com.didichuxing.horoscope.util.Constants.ZK_CONF_PATH

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.ExecutionContext
import scala.util.parsing.json.JSON._
import scala.util.parsing.json.{JSONArray, JSONObject}
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
 *  ZK上编排配置节点组织结构
 *  config
 *    |-- log: 埋点配置
 *    |    |-- config-data
 *    |-- subscription: 订阅配置
 *    |-- experiment: 实验配置
 *    |-- callback: 回调配置
 *    |-- version: 历史版本配置
 *    |      |-- id: 版本id自增生成器
 *    |      |-- config-type
 *    |      |         |-- config-name
 *    |      |         |        |-- version-id: 某版本的配置数据
 */

class ZookeeperConfigStore(curator: CuratorFramework,
  zkClient: ZkClient) extends ConfigStore with TreeCacheListener with Logging {

  import akka.http.scaladsl.server.Directives._
  import ZookeeperConfigStore._
  import JsonSupport._

  private val configChangeListeners = ArrayBuffer[ConfigChangeListener]()

  private var configChecker: ConfigChecker = null

  private val executor = Executors.newSingleThreadExecutor()

  implicit def executionContext: ExecutionContext = ExecutionContext.fromExecutor(executor)

  private val isInitialized: Semaphore = new Semaphore(0)

  private val versionIdGenerator = new DistributeIDGenerator("/version/id", curator)

  // register cache listener
  private val tree: TreeCache = {
    val cache = TreeCache.newBuilder(curator, "/")
      .setCreateParentNodes(true)
      .setExecutor(executor)
      .build()
    cache.getListenable.addListener(this)
    cache.start()

    cache
  }

  isInitialized.acquire()

  override protected def childEvent(client: CuratorFramework, event: TreeCacheEvent): Unit = synchronized {
    import TreeCacheEvent.Type._

    event.getType match {
      case INITIALIZED => isInitialized.release()
      case NODE_ADDED =>
        logging.info(s"node added, path is : ${event.getData.getPath}")
      case NODE_UPDATED =>
        logging.info(s"node updated,path is : ${event.getData.getPath}")
      case NODE_REMOVED =>
        logging.info(s"node removed, path is : ${event.getData.getPath}")
    }

    configChangeListeners.foreach(x => {
      x.onConfUpdate()
    })
  }

  override def getConf(name: String, confType: String): Config = {
    val path = s"${confType}/$name"
    getConfByPath(path)
  }

  override def getConfList(confType: String): List[Config] = {
    getConfListByType(confType)
  }

  override def registerListener(listener: ConfigChangeListener): Unit = {
    configChangeListeners += listener
  }

  override def registerChecker(checker: ConfigChecker): Unit =  {
    configChecker = checker
  }

  override def api: Route = {
    import akka.http.scaladsl.server.Directives._
    concat(
      pathPrefix("configs")(configListRoutes),
      pathPrefix("config")(configRoutes),
      pathPrefix("config")(configCloneRoute),
      pathPrefix("config")(configRollbackRoute),
      pathPrefix("config")(configVersionRoute)
    )
  }

  private def getFullPath(configType: String, configName: String): String = s"/$configType/$configName"

  private def getFullPath(path: String): String = {
    if (path.startsWith("/")) path else s"/$path"
  }

  /** update operation to ZK */
  private def update(path: String, text: String): Unit = {
    val fullPath = getFullPath(path)
    val stat: Stat = curator.checkExists().forPath(fullPath)
    if (stat == null) {
      curator.create().creatingParentsIfNeeded()
        .withMode(CreateMode.PERSISTENT)
        .forPath(fullPath, text.getBytes("UTF-8"))
    } else {
      curator.setData()
        .forPath(fullPath, text.getBytes("UTF-8"))
    }
  }

  /** delete operation to ZK */
  private def zkDelete(path: String): Unit = {
    val fullPath = getFullPath(path)
    val stat = curator.checkExists().forPath(fullPath)
    if (stat != null) {
      curator.delete().deletingChildrenIfNeeded().forPath(fullPath)
    }
  }

  private def read(fullPath: String): String = {
    val stat: Stat = curator.checkExists().forPath(fullPath)
    if (stat != null) {
      val bytes: Array[Byte] = curator.getData.forPath(fullPath)
      if (bytes != null) {
        val result = new String(bytes, "UTF-8")
        result
      } else {
        null
      }
    } else {
      null
    }
  }

  private def cloneConfig(configType: String, configName: String, targetCluster: String): Boolean = {
    val sourcePath = s"${zkClient.getConfigPath()}/${configType}/${configName}"
    val targetPath = s"/${targetCluster}/${ZK_CONF_PATH}/${configType}/${configName}"
    if (!zkClient.exist(sourcePath)) {
      throw new IllegalArgumentException("source config doesn't exist")
    } else if (zkClient.exist(targetPath)) {
      throw new IllegalArgumentException("target config exists")
    } else {
      val data = new String(curator.getData.forPath(getFullPath(configType, configName)))
      // to keep config disabled
      val config = ConfigFactory.parseString(data)
        .withValue("enabled", ConfigValueFactory.fromAnyRef(false))
      val configContent = config.root().render(ConfigRenderOptions.concise())
      info(("msg", s"prepare to copy config: ${configType}/${configName} to cluster: ${targetCluster}, " +
        s"content: ${configContent}"))
      zkClient.create(targetPath)
      zkClient.setData(targetPath, config)
    }
  }

  @throws(classOf[IllegalArgumentException])
  private def getConfByPath(path: String): Config = {
    val fullPath = getFullPath(path)
    val stat: Stat = curator.checkExists().forPath(fullPath)
    if (stat != null) {
      ConfigFactory.parseString(new String(curator.getData.forPath(fullPath), "UTF-8"))
    } else {
      throw new IllegalArgumentException("zk node not exist")
    }
  }

  // @throws(classOf[IllegalArgumentException])
  private def getConfListByType(configType: String): List[Config] = {
    val configTypePath = getFullPath(configType)
    createFolderIfNotExist(configTypePath)
    val result = ListBuffer[Config]()
    val fullPath = getFullPath(configType)
    val stat: Stat = curator.checkExists().forPath(fullPath)
    if (stat != null) {
      val children = curator.getChildren.forPath(fullPath).asScala
      children.foreach(x => {
        result.append(getConfByPath(s"$configType/$x"))
      })
      result.toList
    } else {
      // throw new IllegalArgumentException("file not exist")
      error(("get config", s"path not exist: $fullPath"))
      Nil
    }
  }

  val configListRoutes: Route = {
    concat(
      path(Segment) { configType =>
        get {
          if (configTypeValid(configType)) {
            val configTypePath = s"/${configType}"
            createFolderIfNotExist(configTypePath)
            val list = getChildrenContent(configTypePath)
            val typeName = configType match {
              case LOG_TYPE => "log-conf"
              case SUBSCRIPTION_TYPE => "subscriptions"
              case EXPERIMENT_TYPE => "experiments"
              case CALLBACK_TYPE => "callbacks"
            }
            val sorted = list.sortBy(_.asJsObject.getFields("name").head.convertTo[String])
            val data = JSONObject(Map(typeName -> JSONArray(sorted))).toString()
            complete(HttpResponse(StatusCodes.OK, entity = data))
          } else {
            complete(HttpResponse(StatusCodes.BadRequest, entity = ""))
          }
        }
      }
    )
  }

  val configRoutes: Route = {
    path(Segment / Segment) { (configType, configName) =>
      concat(
        get {
          if (configTypeValid(configType) && !configName.contains('/')) {
            val configTypePath = s"/$configType"
            createFolderIfNotExist(configTypePath)
            val stat: Stat = curator.checkExists().forPath(getFullPath(configType, configName))
            if (stat == null) {
              complete(HttpResponse(StatusCodes.BadRequest, entity = "config not exist"))
            } else {
              val configContent = new String(curator.getData.forPath(getFullPath(configType, configName)))
              complete(HttpResponse(StatusCodes.OK, entity = configContent))
            }
          } else {
            complete(HttpResponse(StatusCodes.BadRequest, entity = "invalid params"))
          }
        },
        put {
          entity(as[String]) { text =>
            info(s"got config to update, ${text}")
            if (!text.isInstanceOf[String] || parseFull(text).isEmpty) {
              complete(HttpResponse(StatusCodes.BadRequest, entity = "invalid conf data"))
            } else {
              Utils.run {
                val original = ConfigFactory.parseString(text)
                if (configChecker != null) configChecker.check(configName, configType, original)
                val newVersion = versionIdGenerator.nextId()
                val newValue = original.withValue("version.id", newVersion.toConfigValue)
                  .withValue("version.comment", "".toConfigValue)
                  .root().render(ConfigRenderOptions.concise())
                // update config data
                update(s"/$configType/$configName", newValue)
                // update config version data
                update(s"/version/$configType/$configName/$newVersion", newValue)
                info(s"config update successfully, ${newValue}")
                complete(HttpResponse(status = StatusCodes.OK, entity = "successfully updated"))
              }
            }
          }
        },
        delete {
          Utils.run {
            if (configTypeValid(configType)) {
              val stat: Stat = curator.checkExists().forPath(getFullPath(configType, configName))
              if (stat == null) {
                complete(HttpResponse(status = StatusCodes.BadRequest, entity = ""))
              } else {
                // delete config data
                zkDelete(getFullPath(configType, configName))
                // delete config version data
                zkDelete(s"/version/$configType/$configName")
                complete(HttpResponse(status = StatusCodes.OK, entity = "successfully deleted"))
              }
            } else {
              complete(HttpResponse(status = StatusCodes.BadRequest, entity = ""))
            }
          }
        }
      )
    }
  }

  val configCloneRoute: Route = {
    post {
      path(Segment / Segment / "clone") { (configType, configName) =>
        entity(as[String]) { cluster =>
          if (configTypeValid(configType)) {
            Utils.run {
              val success = cloneConfig(configType, configName, cluster)
              if (success) {
                complete(HttpResponse(StatusCodes.OK))
              } else {
                complete(HttpResponse(StatusCodes.BadRequest, entity = "clone failed"))
              }
            }
          } else {
            complete(HttpResponse(StatusCodes.BadRequest, entity = "invalid config type"))
          }
        }
      }
    }
  }

  val configRollbackRoute: Route = {
    post {
      path(Segment / Segment / "rollback") { (configType, configName) =>
        entity(as[JsValue]) { json =>
          Utils.run {
            json.asJsObject.getFields("rollback_version", "modify_time", "modify_user") match {
              case Seq(JsNumber(rollbackVersion), JsString(modifyTime), JsString(modifyUser)) =>
                val newVersion = versionIdGenerator.nextId()
                val newValue = getVersionConf(configType, configName, rollbackVersion.longValue())
                  .withValue("version",
                    ConfigValueFactory.fromMap(
                      Map("id" -> newVersion, "comment" -> s"rollback from version ${rollbackVersion}",
                        "modify_time" -> modifyTime, "modify_user" -> modifyUser)))
                  .root().render(ConfigRenderOptions.concise())
                // update config data
                update(s"/$configType/$configName", newValue)
                // update version data
                update(s"/version/$configType/$configName/$newVersion", newValue)
                complete(HttpResponse(StatusCodes.OK))
            }
          }
        }
      }
    }
  }

  val configVersionRoute: Route = {
    concat(
      get {
        path(Segment / Segment / "versions") { (configType, configName) =>
          Utils.run {
            val exist = checkPathExist(s"/version/$configType/$configName")
            if (exist) {
              val list = getChildrenContent(s"/version/$configType/$configName")
              val sorted = list.sortBy(
                _.asJsObject.getFields("version").head.asJsObject
                  .getFields("id").head.convertTo[Long]
              )
              val data = JSONArray(sorted).toString()
              complete(HttpResponse(StatusCodes.OK, entity = data))
            } else {
              complete(HttpResponse(StatusCodes.BadRequest, entity = "conf not exist"))
            }
          }
        }
      },
      get {
        path(Segment / Segment / Segment) {
          { (configType, configName, versionId) =>
            Utils.run {
              val exist = checkPathExist(s"/version/$configType/$configName")
              if (exist) {
                val path = s"/version/$configType/$configName/$versionId"
                val config = getConfByPath(path)
                val data = config.root().render(ConfigRenderOptions.concise())
                complete(HttpResponse(StatusCodes.OK, entity = data))
              } else {
                complete(HttpResponse(StatusCodes.BadRequest, entity = "conf not exist"))
              }
            }
          }
        }
      }
    )
  }

  private def getVersionConf(configType: String, configName: String, version: Long): Config = {
    getConfByPath(s"version/$configType/$configName/$version")
  }

  private def configTypeValid(configType: String): Boolean = {
    configType == LOG_TYPE || configType == SUBSCRIPTION_TYPE || configType == EXPERIMENT_TYPE ||
      configType == CALLBACK_TYPE
  }

  private def checkPathExist(path: String): Boolean = {
    val stat: Stat = curator.checkExists().forPath(path)
    stat != null
  }

  private def createFolderIfNotExist(path: String): Unit = {
    val stat: Stat = curator.checkExists().forPath(path)
    if (stat == null) {
      curator.create().creatingParentsIfNeeded()
        .withMode(CreateMode.PERSISTENT)
        .forPath(path)
    }
  }

  def getChildrenContent(path: String): List[JsValue] = {
    val list = curator.getChildren.forPath(path).asScala
    val result = ListBuffer[JsValue]()
    list.foreach(x => {
      val item = read(s"$path/$x")
      if (item != null) {
        val jsonAst = item.parseJson
        result.append(jsonAst)
      }
    })
    result.result()
  }
}

object ZookeeperConfigStore {
  import FlowConfParser._

  val LOG_TYPE = "log"
  val SUBSCRIPTION_TYPE = "subscription"
  val EXPERIMENT_TYPE = "experiment"
  val CALLBACK_TYPE = "callback"

  // ensure all flow tags are completely defined
  def checkLogConf(config: Config): Unit = {
    val logConf = config.parseLogConf()
    val flows = logConf.fields.filterNot(_.`type` == "tag").map(_.flow).toSet
    val tagFlows = logConf.fields.filter(_.`type` == "tag").map(_.flow).toSet
    val notTagged = flows.find(!tagFlows.contains(_))
    assert(notTagged.isEmpty, s"no tag is defined for flow ${notTagged.get}")
    val fields = logConf.fields.map(_.name)
    assert(fields.distinct.length == fields.length, "config has duplicate field name")
  }

  def checkConfName(name: String, config: Config): Unit = {
    val contentName = config.getString("name")
    assert(contentName == name, "name in content is wrong")
  }

  // ensure that priorities are not duplicated and experimental flow sum do not exceed 1
  def checkExperimentConf(config: Config, existing: Seq[Config]): Unit = {
    val currentName = config.getString("name")
    val total = Seq(config) ++ existing.filterNot(_.getString("name") == currentName)
    val trafficSum = total.map { conf =>
      val enabled = conf.getBoolean("enabled")
      val traffic = conf.getIntList("traffic").asScala
      val range = traffic(1) - traffic.head
      (enabled, range)
    }.filter(_._1).map(_._2).sum
    val priorities = total.map(_.getInt("priority"))
    assert(priorities.toSet.size == priorities.length, s"duplicate experiment priority")
    // assert(trafficSum <= 100, s"traffic ratio sum of all enabled experiment exceed than 100%")
  }

  implicit class ConfigConverter(any: Any) {
    def toConfigValue: ConfigValue = {
      ConfigValueFactory.fromAnyRef(any)
    }
  }
}
