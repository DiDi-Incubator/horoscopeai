package com.didichuxing.horoscope.service.storage

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Route
import com.didichuxing.horoscope.core.{ConfigChangeListener, ConfigStore}
import com.didichuxing.horoscope.util.Logging
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache._
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.data.Stat
import spray.json.{JsValue, _}

import java.util.concurrent.{Executors, Semaphore}
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.ExecutionContext
import scala.util.parsing.json.JSON._

class ZookeeperConfigStore(curator: CuratorFramework) extends ConfigStore with TreeCacheListener with Logging {

  import akka.http.scaladsl.server.Directives._

  private val configChangeListeners = ArrayBuffer[ConfigChangeListener]()

  private val executor = Executors.newSingleThreadExecutor()

  implicit def executionContext: ExecutionContext = ExecutionContext.fromExecutor(executor)

  private val isInitialized: Semaphore = new Semaphore(0)

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
    notifyAllListeners()
  }

  override def getLogConf(name: String): Config = {
    val path = s"$LOG_TYPE/$name"
    getConfByPath(path)
  }

  override def getExperimentConf(name: String): Config = {
    val path = s"${SUBSCRIPTION_TYPE}/$name"
    getConfByPath(path)
  }

  override def getSubscriptionConf(name: String): Config = {
    val path = s"$EXPERIMENT_TYPE/$name"
    getConfByPath(path)
  }

  override def getLogConfList: List[Config] = {
    getConfListByType(LOG_TYPE)
  }

  override def getExperimentConfList: List[Config] = {
    getConfListByType(EXPERIMENT_TYPE)
  }

  override def getSubscriptionConfList: List[Config] = {
    getConfListByType(SUBSCRIPTION_TYPE)
  }

  override def register(listener: ConfigChangeListener): Unit = {
    configChangeListeners += listener
  }

  override def api: Route = {
    import akka.http.scaladsl.server.Directives._
    concat(
      pathPrefix("configs")(configListRoutes),
      pathPrefix("config")(configRoutes)
    )
  }

  private def getFullPath(configType: String, configName: String): String = s"/$configType/$configName"

  private def getFullPath(path: String): String = s"/$path"

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

  @throws(classOf[IllegalArgumentException])
  private def getConfByPath(path: String): Config = {
    val fullPath = getFullPath(path)
    val stat: Stat = curator.checkExists().forPath(fullPath)
    if (stat != null) {
      ConfigFactory.parseString(new String(curator.getData.forPath(fullPath), "UTF-8"))
    } else {
      throw new IllegalArgumentException("file not exist")
    }
  }

  // @throws(classOf[IllegalArgumentException])
  private def getConfListByType(configType: String): List[Config] = {
    createFolderIfNotExist(configType)
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
      error(("get config", s"path not exist: ${fullPath}"))
      Nil
    }
  }



  val configListRoutes: Route = {
    concat(
      path(Segment) { configType =>
        get {
          if (configType == LOG_TYPE || configType == SUBSCRIPTION_TYPE || configType == EXPERIMENT_TYPE) {
            createFolderIfNotExist(configType)
            val contentList = getChildrenContent(configType)
            complete(HttpResponse(StatusCodes.OK, entity = contentList))
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
          if (configName.contains('/')) {
            complete(HttpResponse(StatusCodes.BadRequest, entity = ""))
          }
          if (configType == LOG_TYPE || configType == SUBSCRIPTION_TYPE || configType == EXPERIMENT_TYPE) {
            createFolderIfNotExist(configType)
            val stat: Stat = curator.checkExists().forPath(getFullPath(configType, configName))
            if (stat == null) {
              complete(HttpResponse(StatusCodes.BadRequest, entity = ""))
            } else {
              val configContent = new String(curator.getData.forPath(getFullPath(configType, configName)))
              complete(HttpResponse(StatusCodes.OK, entity = configContent))
            }
          } else {
            complete(HttpResponse(StatusCodes.BadRequest, entity = ""))
          }
        },
        put {
          entity(as[String]) { text =>
            if (!text.isInstanceOf[String] || parseFull(text).isEmpty) {
              complete(HttpResponse(StatusCodes.BadRequest, entity = ""))
            } else {
              update(s"$configType/$configName", text)
            }
            complete(HttpResponse(status = StatusCodes.OK, entity = "successfully updated"))
          }
        },
        delete {
          if (configType == LOG_TYPE || configType == SUBSCRIPTION_TYPE || configType == EXPERIMENT_TYPE) {
            val stat: Stat = curator.checkExists().forPath(getFullPath(configType, configName))
            if (stat == null) {
              complete(HttpResponse(StatusCodes.BadRequest, entity = ""))
            } else {
              curator.delete().deletingChildrenIfNeeded().forPath(getFullPath(configType, configName))
              complete(HttpResponse(status = StatusCodes.OK, entity = "successfully deleted"))
            }
          } else {
            complete(HttpResponse(StatusCodes.BadRequest, entity = ""))
          }
        }
      )
    }
  }

  private def notifyAllListeners(): Unit = {
    configChangeListeners.foreach(x => {
      x.onConfUpdate()
    })
  }

  private def createFolderIfNotExist(configType: String): Unit = {
    val fullPath = getFullPath(configType)
    val stat: Stat = curator.checkExists().forPath(fullPath)
    if (stat == null) {
      curator.create().creatingParentsIfNeeded()
        .withMode(CreateMode.PERSISTENT)
        .forPath(fullPath)
    }
  }

  def getChildrenContent(configType: String): String = {
    createFolderIfNotExist(configType)
    val path = getFullPath(configType)
    val list = curator.getChildren.forPath(path).asScala
    val typeName = configType match {
      case LOG_TYPE => "log-conf"
      case SUBSCRIPTION_TYPE => "subscriptions"
      case EXPERIMENT_TYPE => "experiments"
    }
    val result = ListBuffer[JsValue]()
    list.foreach(x => {
      val item = read(s"$path/$x")
      if (item != null) {
        val jsonAst = item.parseJson
        result.append(jsonAst)
      }
    })
    val sortedList = result.sortBy(_.asJsObject.getFields("name").head.toString)
    val map = Map(
      typeName -> scala.util.parsing.json.JSONArray(sortedList.toList)
    )
    scala.util.parsing.json.JSONObject(map).toString()
  }

  val LOG_TYPE = "log"
  val SUBSCRIPTION_TYPE = "subscription"
  val EXPERIMENT_TYPE = "experiment"
}
