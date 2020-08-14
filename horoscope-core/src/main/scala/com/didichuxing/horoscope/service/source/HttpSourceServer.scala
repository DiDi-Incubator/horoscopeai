/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{get, _}
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import com.didichuxing.horoscope.core.Source
import com.didichuxing.horoscope.runtime.{BooleanValue, NULL, Text, Value, ValueDict}
import com.didichuxing.horoscope.runtime.convert.ValueTypeAdapter
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.service.cluster.FlowClient
import com.didichuxing.horoscope.service.source.EventProcessErrorCode._
import com.didichuxing.horoscope.util.{Logging, SystemLog}
import com.google.gson.GsonBuilder

import scala.collection.mutable
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import scala.util.Try

class HttpSourceServer(implicit ctx: ApplicationContext) extends Logging {

  implicit val actorSystem = ctx.system
  implicit val ec = ctx.sourceExecutionContext.getExecutionContext()
  implicit val materializer = ActorMaterializer()
  implicit private val gson = new GsonBuilder()
    .registerTypeHierarchyAdapter(classOf[Value], new ValueTypeAdapter)
    .create()
  private var bindingFuture: Future[ServerBinding] = _
  private var sourceCtrl: SourceControl = _
  //sources的引用，必须用mutable map
  private var sources: mutable.Map[String, Source] = _

  case class ErrorResp(code: Int, description: String)

  case class PostResult(code: ErrorCode, data: Value = NULL)

  def start(sources: mutable.Map[String, Source], client: FlowClient = null): Unit = {
    if (client != null) {
      sourceCtrl = new SourceControl(client)(ctx.config, actorSystem)
    }
    this.sources = sources
    val route: Route = schedule() ~ ctrl()
    val hostConf = Try(ctx.config.getString("akka.remote.netty.tcp.hostname")).getOrElse("localhost")
    val host = if (hostConf == "") "localhost" else hostConf
    val port = Try(ctx.config.getInt("horoscope.source.http.port")).getOrElse(8081)
    bindingFuture = Http().bindAndHandle(route, host, port)
    info(("msg", "start http source server"), ("host", host), ("port", port))
  }

  private def schedule(): Route = {
    concat(
      post {
        path("schedule" / Segment / Segment / Segment / Remaining) { (source, trace, time, flowName) =>
          entity(as[String]) { json =>
            SystemLog.create()
            debug(("msg", "http source sync"), ("flowName", flowName), ("body", json))
            onComplete(doSchedule(source, trace, flowName, json)) {
              result => complete(jsonResponse(result.getOrElse(PostResult(UnknownError))))
            }
          }
        }
      },
      post {
        path("scheduleAsync" / Segment / Segment / Segment / Remaining) { (source, trace, time, flowName) =>
          entity(as[String]) { json =>
            SystemLog.create()
            debug(("msg", "http source async"), ("flowName", flowName), ("body", json))
            onComplete(doScheduleAsync(source, trace, flowName, json)) {
              result => complete(jsonResponse(result.getOrElse(PostResult(UnknownError))))
            }
          }
        }
      },
      get {
        path("schedule" / Segment / Segment / Segment / Remaining) { (source, trace, time, flowName) =>
          parameterMap { params =>
            SystemLog.create()
            debug(("msg", "http source sync"), ("flowName", flowName), ("params", params))
            onComplete(doSchedule(source, trace, flowName, params)) {
              result => complete(jsonResponse(result.getOrElse(PostResult(UnknownError))))
            }
          }
        }
      },
      get {
        path("scheduleAsync" / Segment / Segment / Segment / Remaining) { (source, trace, time, flowName) =>
          parameterMap { params =>
            SystemLog.create()
            debug(("msg", "http source async"), ("flowName", flowName), ("params", params))
            onComplete(doScheduleAsync(source, trace, flowName, params)) {
              result => complete(jsonResponse(result.getOrElse(PostResult(UnknownError))))
            }
          }
        }
      }
    )
  }

  private def ctrl(): Route = {
    concat(
      post {
        path("sourceCtrl") {
          entity(as[String]) { json =>
            SystemLog.create()
            debug(("msg", "source control"), ("command", json))
            onComplete(doSourceCtrl(json)) {
              result =>
                complete {
                  val httpEntity = HttpEntity(ContentTypes.`application/json`, gson.toJson(result))
                  HttpResponse().withEntity(httpEntity)
                }
            }
          }
        }
      }
    )
  }

  private def jsonResponse(result: PostResult): HttpResponse = {
    result.code match {
      case Success =>
        val httpEntity = HttpEntity(ContentTypes.`application/json`, gson.toJson(result.data))
        HttpResponse().withEntity(httpEntity)
      case err@_ =>
        val errResponse = ErrorResp(err.id, err.toString)
        val json = gson.toJson(errResponse)
        val httpEntity = HttpEntity(ContentTypes.`application/json`, json)
        HttpResponse(StatusCodes.BadRequest).withEntity(httpEntity)
    }
  }

  def doSchedule(sourceName: String, trace: String, flowName: String, body: Any): Future[PostResult] = {
    val p = Promise[PostResult]()
    Future {
      val source = sources.get(sourceName).getOrElse(null)
      if (source != null && source.isInstanceOf[HttpSource]) {
        try {
          val instance = source.asInstanceOf[HttpSource]
            .pushSync(trace, flowName, body)
          p.success(PostResult(Success, Value(instance)))
        } catch {
          case EventProcessException(err) =>
            p.success(PostResult(err))
          case _ =>
            p.success(PostResult(UnknownError))
        }
      } else {
        p.success(PostResult(SourceError))
      }
    }
    p.future
  }

  def doScheduleAsync(sourceName: String, trace: String, flowName: String, body: Any): Future[PostResult] = {
    val p = Promise[PostResult]()
    Future {
      val source = sources.get(sourceName).getOrElse(null)
      if (source != null && source.isInstanceOf[HttpSource]) {
        try {
          val events = source.asInstanceOf[HttpSource]
            .pushAsync(trace, flowName, List[Any](body))
          val event = events(0)
          if (event == null) {
            p.success(PostResult(FormatError))
          } else {
            p.success(PostResult(Success, Value(event)))
          }
        } catch {
          case EventProcessException(err) =>
            p.success(PostResult(err))
          case ex: Throwable =>
            ex.printStackTrace()
            p.success(PostResult(UnknownError))
        }
      } else {
        p.success(PostResult(SourceError))
      }
    }
    p.future
  }

  def doSourceCtrl(json: String): Future[Value] = {
    val p = Promise[Value]()
    Future {
      val value = gson.fromJson(json, classOf[Value])
      if (sourceCtrl == null) {
        p.success(Value("not support"))
      } else {
        try {
          val event = value.as[ValueDict]
          val command = event.visit("command").as[Text].underlying
          val result = command match {
            case "start" =>
              val source = event.visit("source").as[Text].underlying
              val remote = Try(event.visit("remote").as[BooleanValue].underlying).getOrElse(true)
              sourceCtrl.startSource(source, remote)
              Value(s"$source start success")
            case "stop" =>
              val source = event.visit("source").as[Text].underlying
              val remote = Try(event.visit("remote").as[BooleanValue].underlying).getOrElse(true)
              sourceCtrl.stopSource(source, remote)
              Value(s"$source stop success")
            case "remove" =>
              val source = event.visit("source").as[Text].underlying
              val remote = Try(event.visit("remote").as[BooleanValue].underlying).getOrElse(true)
              sourceCtrl.removeSource(source, remote)
              Value(s"$source remove success")
            case "register" =>
              val config = event.visit("config").as[Text].underlying
              val remote = Try(event.visit("remote").as[BooleanValue].underlying).getOrElse(true)
              sourceCtrl.registerSource(config, remote)
              Value(s"register success : $config")
            case "running" =>
              Value(sourceCtrl.listAllRunning())
            case "list" =>
              Value(sourceCtrl.listAllRegistered())
            case _ =>
              Value("unknow command")
          }
          p.success(result)
        } catch {
          case ex: Throwable =>
            ex.printStackTrace()
            p.success(Value(ex.getMessage))
        }
      }
    }
    p.future
  }

  def stop(): Unit = {
    // trigger unbinding from the port
    info(("msg", "stop http source server"))
    Await.ready(bindingFuture.flatMap(_.terminate(3 seconds)), 5 seconds)
  }

}

object HttpSourceServer {
  val HTTP_SOURCE_NAME = "http_source"
}
