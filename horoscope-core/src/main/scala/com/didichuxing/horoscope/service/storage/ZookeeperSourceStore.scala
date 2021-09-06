package com.didichuxing.horoscope.service.storage

import java.util.NoSuchElementException

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives.entity
import akka.http.scaladsl.server.Route
import com.didichuxing.horoscope.core.SourceStore
import com.didichuxing.horoscope.service.resource.ZkClient
import com.didichuxing.horoscope.util.Constants.ZK_SOURCE_PATH
import com.didichuxing.horoscope.util.{Logging, SystemLog, Utils}
import com.typesafe.config._
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type
import org.apache.curator.framework.recipes.cache.{PathChildrenCacheEvent, PathChildrenCacheListener}

import scala.collection.mutable.ListBuffer
import scala.util.Try

class ZkSourceStore(zkClient: ZkClient, sourceListener: SourceListener)
  extends SourceStore with PathChildrenCacheListener with Logging {

  val sourcePath = zkClient.getSourcePath()
  zkClient.watchNode(sourcePath, this)

  override def putSource(source: Config): Unit = {
    val sourceName = source.getString("source-name")
    val path = s"$sourcePath/$sourceName"
    zkClient.create(path)
    zkClient.setData(path, source)
  }

  override def removeSource(sourceName: String): Unit = {
    val path = s"$sourcePath/$sourceName"
    if (zkClient.exist(path)) {
      zkClient.delete(path)
    } else {
      throw new NoSuchElementException
    }
  }

  override def getSource(sourceName: String): Config = {
    val path = s"$sourcePath/$sourceName"
    val source = zkClient.getData(path).get
    ConfigFactory.parseString(source)
  }

  override def getSources(): Seq[Config] = {
    val sources = ListBuffer[Config]()
    zkClient.getChild(sourcePath).foreach(
      sourceName => {
        val source = getSource(sourceName)
        sources.append(source)
      }
    )
    sources
  }

  override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
    SystemLog.create()
    event.getType match {
      case Type.CHILD_ADDED =>
        val content = new String(event.getData.getData, "UTF-8")
        val config = ConfigFactory.parseString(content)
        sourceListener.onRegister(config)

      case Type.CHILD_UPDATED =>
        val content = new String(event.getData.getData, "UTF-8")
        val config = ConfigFactory.parseString(content)
        try {
          sourceListener.onUpdate(config.getString("source-name"), config)
        } catch {
          case ex: Exception =>
            ex.printStackTrace()
        }

      case Type.CHILD_REMOVED =>
        val content = new String(event.getData.getData, "UTF-8")
        val config = ConfigFactory.parseString(content)
        try {
          sourceListener.onRemove(config.getString("source-name"))
        } catch {
          case ex: Exception =>
            ex.printStackTrace()
        }

      case _ =>
        info(("msg", "unknown event type"), ("event type", event.getType))
    }
  }

  private def cloneSource(sourceName: String, targetCluster: String): Boolean = {
    // to keep source disabled
    val source = getSource(sourceName).withValue("parameter.disabled", ConfigValueFactory.fromAnyRef(true))
    assert(sourceName == source.getString("source-name"), "source-name is wrong")
    val targetPath = s"/${targetCluster}/$ZK_SOURCE_PATH/$sourceName"
    if (zkClient.exist(targetPath)) {
      throw new IllegalArgumentException("source config exists")
    }
    zkClient.create(targetPath)
    zkClient.setData(targetPath, source)
  }

  //scalastyle:off
  override def api: Route = {
    import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
    import akka.http.scaladsl.server.Directives._
    import spray.json._
    import DefaultJsonProtocol._
    concat(
      get {
        //获得集群中泛源状态
        path(Remaining) { source =>
          SystemLog.create()
          debug(("msg", "get source"), ("source", source))
          complete(
            try {
              getSource(source).root().render(ConfigRenderOptions.concise()).parseJson
            } catch {
              case _: NoSuchElementException =>
                HttpResponse(404, entity = s"unknown source $source")
            }
          )
        }
      },
      delete {
        //删除泛源
        path(Remaining) { source =>
          SystemLog.create()
          debug(("msg", "remove source"), ("source", source))
          complete(
            try {
              removeSource(source)
              StatusCodes.OK
            } catch {
              case _: NoSuchElementException =>
                HttpResponse(404, entity = s"source $source not exist ")
            }
          )
        }
      },
      put {
        //启动/暂停泛源
        path(Segment / "enabled") { sourceName =>
          entity(as[String]) { enabled =>
            info(("msg", s"enable/disable source, ${sourceName}, ${enabled}"))
            complete(
              Try {
                val disabled = !Try(enabled.toBoolean).getOrElse(false)
                val source = getSource(sourceName).withValue("parameter.disabled", ConfigValueFactory.fromAnyRef(disabled))
                putSource(source)
                StatusCodes.OK
              }
            )
          }
        }
      },
      put {
        //启动/暂停泛源
        path(Segment / "concurrency") { sourceName =>
          entity(as[String]) { concurrency =>
            info(("msg", s"update source concurrency"), ("source", sourceName), ("concurrency", concurrency))
            complete(
              Try {
                val permits = Try(concurrency.toInt).getOrElse(50)
                val source = getSource(sourceName).withValue("parameter.backpress.permits", ConfigValueFactory.fromAnyRef(permits))
                putSource(source)
                StatusCodes.OK
              }
            )
          }
        }
      },
      post {
        path(Segment / "clone") { sourceName =>
          entity(as[String]) { cluster =>
            Utils.run {
              val success = cloneSource(sourceName, cluster)
              if (success) {
                complete(HttpResponse(StatusCodes.OK))
              } else {
                complete(HttpResponse(StatusCodes.BadRequest, entity = "clone failed"))
              }
            }
          }
        }
      },
      get {
        //获得集群处理的泛源列表
        SystemLog.create()
        debug(("msg", "get source list"))
        val sources = getSources().map(config => config.getString("source-name"))
        complete(sources.toJson)
      },
      put {
        //添加泛源 post
        entity(as[String]) { content =>
          SystemLog.create()
          info(("msg", s"got source config to update, ${content}"))
          val config = ConfigFactory.parseString(content)
          complete(
            try {
              putSource(config)
              info(("msg", s"source config update successfully, ${content}"))
              StatusCodes.OK
            } catch {
              case _: ConfigException =>
                HttpResponse(400, entity = s"config error $content")
            }
          )
        }
      }
    )
  }
}

trait SourceListener {
  //新泛源注册
  def onRegister(source: Config)

  //删除泛源
  def onRemove(sourceName: String)

  //泛源更新
  def onUpdate(sourceName: String, source: Config)
}
