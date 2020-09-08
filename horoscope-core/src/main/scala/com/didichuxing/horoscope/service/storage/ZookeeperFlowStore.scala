/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.storage

import java.nio.file.{FileVisitOption, Files, Path}
import java.util.concurrent.{Executors, Semaphore}
import java.util.function.Consumer

import akka.http.scaladsl.server.Route
import com.didichuxing.horoscope.core.FlowDslMessage.FlowDef
import com.didichuxing.horoscope.core.FlowStore
import com.didichuxing.horoscope.dsl.FlowCompiler
import com.didichuxing.horoscope.util.Logging
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{TreeCache, TreeCacheEvent, TreeCacheListener}

import scala.concurrent.ExecutionContext
import scala.util.Try

class ZookeeperFlowStore(curator: CuratorFramework) extends FlowStore with TreeCacheListener with Logging {
  private val executor = Executors.newSingleThreadExecutor()

  implicit def executionContext: ExecutionContext = ExecutionContext.fromExecutor(executor)

  private val isInitialized: Semaphore = new Semaphore(0)

  private val tree: TreeCache = {
    val cache = TreeCache.newBuilder(curator.usingNamespace(curator.getNamespace + "/tree"), "/")
      .setCreateParentNodes(true)
      .setExecutor(executor)
      .build()
    cache.getListenable.addListener(this)
    cache.start()

    cache
  }

  var flows: Map[String, FlowDef] = Map.empty

  isInitialized.acquire()

  protected def childEvent(client: CuratorFramework, event: TreeCacheEvent): Unit = synchronized {
    import TreeCacheEvent.Type._

    if (event.getType == INITIALIZED) {
      isInitialized.release()
    }

    if (event.getType == NODE_ADDED || event.getType == NODE_UPDATED) {
      val data = event.getData
      if (data.getData != null && data.getData.nonEmpty) {
        flows += data.getPath -> FlowCompiler.compile(new String(data.getData))
      }
    }

    if (event.getType == NODE_REMOVED) {
      flows -= event.getData.getPath
    }
  }

  private def update(text: String, name: Option[String] = None): Unit = {
    val flow = FlowCompiler.compile(text)
    require(name.forall(_ == flow.getName))

    val path = "/tree" + flow.getName
    curator.createContainers(path)
    curator.setData().forPath(path, text.getBytes)
  }

  def load(path: java.nio.file.Path): Unit = {
    val loader = new Consumer[Path] {
      def accept(file: Path): Unit =
        if (Files.isReadable(file) && file.toFile.getName.endsWith(".flow")) {
          try {
            update(new String(Files.readAllBytes(file)))
          } catch {
            case exception: Exception =>
              logging.error(s"fail to load $file", exception)
          }
        }
    }
    if (Files.isDirectory(path)) {
      Files.walk(path, FileVisitOption.FOLLOW_LINKS).forEach(loader)
    } else {
      loader.accept(path)
    }
  }

  override def getFlowByName(name: String): FlowDef = flows(name)

  override def api: Route = {
    import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
    import akka.http.scaladsl.model._
    import akka.http.scaladsl.server.Directives._
    import spray.json._
    import DefaultJsonProtocol._

    import scala.collection.JavaConversions._

    path("tree" / Remaining) { remaining =>
      val suffix = "/" + remaining

      if (suffix.endsWith("/")) {
        // dir operations
        val path = if (suffix.length <= 1) suffix else suffix.substring(0, suffix.length - 1)
        get {
          val children = tree.getCurrentChildren(path).toList.flatMap({ case (key, value) =>
            val fullPath = suffix + key
            var items: List[String] = Nil

            if (tree.getCurrentChildren(fullPath) != null && tree.getCurrentChildren(fullPath).nonEmpty) {
              items ::= fullPath + "/"
            }
            if (value.getData != null && value.getData.nonEmpty) {
              items ::= fullPath
            }
            items
          })
          complete(children.toJson)
        }
      } else {
        // flow operations
        concat(
          get {
            complete(new String(tree.getCurrentData(suffix).getData))
          },
          put {
            entity(as[String]) { value =>
              complete(Try {
                update(value, Some(suffix))
                StatusCodes.OK
              })
            }
          },
          delete {
            complete(Try {
              curator.delete().deletingChildrenIfNeeded().forPath("/tree" + suffix)
              StatusCodes.OK
            })
          }
        )
      }
    }
  }
}