/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.api

import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.ExceptionHandler
import akka.stream.ActorMaterializer
import com.didichuxing.horoscope.core.Source
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.service.cluster.FlowClient
import com.didichuxing.horoscope.util.Logging
import com.didichuxing.horoscope.service.storage.{ZkSourceStore, ZookeeperClusterStore}
import com.didichuxing.horoscope.util.Utils._

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

class HttpServer(implicit ctx: ApplicationContext) extends Logging {

  implicit val actorSystem = ctx.system
  implicit val ec = ctx.sourceExecutionContext.getExecutionContext()
  implicit val materializer = ActorMaterializer()
  private var bindingFuture: Future[ServerBinding] = _

  def start(sources: mutable.Map[String, Source], client: FlowClient = null): Unit = {
    implicit def apiExceptionHandler = ExceptionHandler {
      case cause: Throwable =>
        extractUri { _ =>
          complete(HttpResponse(StatusCodes.InternalServerError, entity = printStackTraceStr(cause)))
        }
    }

    val scheduleCtrl = new ScheduleController(sources)
    val route = if (client == null) {
      pathPrefix("api") {
        concat(
          pathPrefix("schedule") {
            scheduleCtrl.api
          }
        )
      }
    } else {
      val clusterStore = ZookeeperClusterStore(ctx.zkClient, ctx.resourceManager)
      val sourceStore = new ZkSourceStore(ctx.zkClient, client)
      pathPrefix("api") {
        concat(
          pathPrefix("schedule") {
            scheduleCtrl.api
          },
          pathPrefix("flow") {
            ctx.flowStore.api
          },
          pathPrefix("expression") {
            ctx.flowStore.getBuiltIn.api
          },
          pathPrefix("sources") {
            sourceStore.api
          },
          pathPrefix("clusters") {
            clusterStore.api
          },
          pathPrefix("storage") {
            ctx.traceStore.api
          },
          pathPrefix("logs") {
            ctx.odsLogger.api
          },
          pathPrefix("file") {
            ctx.fileStore.api
          },
          pathPrefix("choreography") {
            ctx.configStore.api
          }
        )
      }
    }
    val hostConf = Try(ctx.config.getString("akka.remote.netty.tcp.hostname")).getOrElse("localhost")
    val host = if (hostConf == "") "localhost" else hostConf
    val port = Try(ctx.config.getInt("horoscope.zookeeper.cluster.api-port")).getOrElse(8062)
    bindingFuture = Http().bindAndHandle(route, host, port)
    info(("msg", "start http api server"), ("host", host), ("port", port))
  }

  def stop(): Unit = {
    // trigger unbinding from the port
    info(("msg", "stop http api server"))
    Await.ready(bindingFuture.flatMap(_.terminate(3 seconds)), 5 seconds)
  }

}
