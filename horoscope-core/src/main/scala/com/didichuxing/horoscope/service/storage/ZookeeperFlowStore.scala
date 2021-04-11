/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.storage

import java.io.ByteArrayOutputStream
import java.nio.file.{FileVisitOption, Files, Path}
import java.util.concurrent.{Executors, Semaphore}
import java.util.function.Consumer
import java.util.zip.{ZipEntry, ZipOutputStream}

import akka.http.scaladsl.model.headers.{ContentDispositionTypes, `Content-Disposition`}
import akka.http.scaladsl.model.{HttpEntity, HttpResponse, MediaTypes, StatusCodes}
import akka.http.scaladsl.server.{Route, StandardRoute}
import com.didichuxing.horoscope.core.FlowDslMessage.{CompositorDef, FlowDef}
import com.didichuxing.horoscope.core.{Compositor, Flow, FlowStore}
import com.didichuxing.horoscope.dsl.FlowCompiler
import com.didichuxing.horoscope.runtime.{Value, ValueDict}
import com.didichuxing.horoscope.runtime.expression.{BuiltIn, DefaultBuiltIn, SimpleBuiltIn}
import com.didichuxing.horoscope.util.Logging
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{TreeCache, TreeCacheEvent, TreeCacheListener}

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

class ZookeeperFlowStore(curator: CuratorFramework) extends FlowStore with TreeCacheListener with Logging {
  import ZookeeperFlowStore._

  import scala.collection.JavaConversions._

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
        try {
          flows += data.getPath -> FlowCompiler.compile(new String(data.getData))
        } catch {
          case e: Exception =>
            logging.error(s"fail to parse flow ${data.getPath}", e)
        }
      }
    }

    if (event.getType == NODE_REMOVED) {
      flows -= event.getData.getPath
    }
  }

  private def update(name: String, text: String): Unit = {
    val path = "/tree" + name
    curator.createContainers(path)
    curator.setData().forPath(path, text.getBytes)
  }

  // for unit test
  def load(path: java.nio.file.Path): Unit = {
    val loader = new Consumer[Path] {
      def accept(file: Path): Unit =
        if (Files.isReadable(file) && file.toFile.getName.endsWith(".flow")) {
          try {
            val text = new String(Files.readAllBytes(file))
            update(checkSyntax(text).name, text)
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

  override def getBuiltIn: BuiltIn = DefaultBuiltIn.defaultBuiltin

  override def api: Route = {
    import akka.http.scaladsl.server.Directives._
    import com.didichuxing.horoscope.runtime.Implicits._

    path("tree" ~ Remaining) {
      case "" =>
        concat(
          get {
            complete(Value(walk()))
          },
          post {
            extractRequestContext { context =>
              import context.materializer
              fileUpload("file") { case (_, byteSource) =>
                onSuccess(byteSource.runFold("")(_ + _.utf8String)) { text =>
                  run { update(checkSyntax(text).name, text) }
                }
              }
            }
          }
        )

      case remaining: String if remaining.endsWith("/") =>
        val path = if (remaining == "/") remaining else remaining.init
        complete(Value(listChildren(path)))

      case remaining: String if remaining.endsWith("/...") =>
        complete(archive(remaining.split('/').filterNot(str => str.isEmpty || str == "...")))

      case remaining: String =>
        concat(
          get {
            complete(new String(tree.getCurrentData(remaining).getData))
          },
          put {
            entity(as[String]) { text =>
              run {
                require(checkSyntax(text).name == remaining, "wrong flow name in header")
                update(remaining, text)
              }
            }
          },
          delete {
            curator.delete().deletingChildrenIfNeeded().forPath("/tree" + remaining)
            complete(StatusCodes.OK)
          }
        )
    }
  }

  def run(block: => Unit): StandardRoute = {
    import akka.http.scaladsl.server.Directives._
    try {
      block
      complete(StatusCodes.OK)
    } catch {
      case e: Exception =>
        complete(StatusCodes.NotAcceptable, e.getMessage)
    }
  }

  def archive(segments: Seq[String]): HttpResponse = {
    val output = new ByteArrayOutputStream()
    val stream = new ZipOutputStream(output)

    def doArchive(node: Node): Unit = {
      if (node.flow != null && node.flow.nonEmpty) {
        val path = node.flow
        stream.putNextEntry(new ZipEntry(node.flow + ".flow"))
        stream.write(tree.getCurrentData(path).getData)
        stream.closeEntry()
      }
      node.children.foreach(doArchive)
    }

    doArchive(walk(segments))
    stream.close()

    HttpResponse()
      .withEntity(HttpEntity(MediaTypes.`application/zip`, output.toByteArray))
      .addHeader(`Content-Disposition`(ContentDispositionTypes.inline, Map("filename" -> "infoflow.zip")))
  }

  def walk(segments: Seq[String] = Nil): Node = {
    val path = segments.mkString("/", "/", "")
    val data = tree.getCurrentData(path)
    val flowName: String = if (data != null && data.getData != null && data.getData.nonEmpty) {
      path
    } else {
      null
    }

    val children = tree.getCurrentChildren(path).keysIterator.map(
      child => walk(segments :+ child)
    ).toArray

    Node(segments.lastOption.getOrElse("/"), flowName, children)
  }

  def listChildren(path: String): Seq[String] = {
    tree.getCurrentChildren(path).toList.flatMap({ case (key, value) =>
      val fullPath = if (path == "/") s"/$key" else s"$path/$key"
      var items: List[String] = Nil

      if (tree.getCurrentChildren(fullPath) != null && tree.getCurrentChildren(fullPath).nonEmpty) {
        items ::= fullPath + "/"
      }
      if (value.getData != null && value.getData.nonEmpty) {
        items ::= fullPath
      }
      items
    })
  }
}

object ZookeeperFlowStore {
  case class Node(name: String, flow: String, children: Seq[Node])

  def checkSyntax(text: String): Flow = {
    implicit def buildCompositor(flow: String, compositorDef: CompositorDef): Compositor = new Compositor {
      def composite(args: ValueDict): Future[Value] = {
        Future.failed(new NotImplementedError())
      }
    }

    // scalastyle:off
    implicit val builtin: BuiltIn = new SimpleBuiltIn(Map.empty, Map.empty)

    Flow(FlowCompiler.compile(text))
  }
}
