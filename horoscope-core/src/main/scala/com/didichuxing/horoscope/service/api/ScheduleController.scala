package com.didichuxing.horoscope.service.api

import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.server.Route
import com.didichuxing.horoscope.core.FlowRuntimeMessage.{FlowEvent, FlowValue, TraceVariable}
import com.didichuxing.horoscope.core.{Source, SyncEventBus}
import com.didichuxing.horoscope.runtime.Implicits.gson
import com.didichuxing.horoscope.service.source.EventProcessErrorCode._
import com.didichuxing.horoscope.service.source.EventProcessException
import com.didichuxing.horoscope.util.{Logging, SystemLog}
import com.didichuxing.horoscope.runtime.Value
import com.didichuxing.horoscope.util.Utils.{getEventId, getTraceId}

import scala.collection.mutable
import scala.concurrent.{ExecutionContextExecutorService, Future, Promise}
import scala.util.Try

class ScheduleController(sources: mutable.Map[String, Source])
                        (implicit ec: ExecutionContextExecutorService) extends Logging {
  //scalastyle:off
  def api: Route = {
    import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
    import akka.http.scaladsl.server.Directives._
    import spray.json._
    concat(
      post {
        path(Segment / Segment / Segment / Remaining) { (source, trace, time, flowName) =>
          SystemLog.create()
          extractRequestEntity { body =>
            if (body.getContentType().mediaType == MediaTypes.`application/x-www-form-urlencoded`) {
              formFieldMap { params =>
                debug(("msg", "http source sync"), ("flowName", flowName), ("params", params))
                onComplete(doScheduleSync(source, trace, flowName, params)) {
                  result =>
                    complete(
                      gson.toJson(result.get).parseJson
                    )
                }
              }
            } else {
              entity(as[String]) { json =>
                debug(("msg", "http source sync"), ("flowName", flowName), ("body", json))
                onComplete(doScheduleSync(source, trace, flowName, json)) {
                  result =>
                    complete(
                      gson.toJson(result.get).parseJson
                    )
                }
              }
            }
          }

        }
      },
      post {
        path("async" / Segment / Segment / Segment / Remaining) { (source, trace, time, flowName) =>
          entity(as[String]) { json =>
            SystemLog.create()
            debug(("msg", "http source async"), ("flowName", flowName), ("body", json))
            onComplete(doScheduleAsync(source, trace, flowName, json)) {
              result =>
                complete(
                  gson.toJson(result.get).parseJson
                )
            }
          }
        }
      },
      get {
        path(Segment / Segment / Segment / Remaining) { (source, trace, time, flowName) =>
          parameterMap { params =>
            SystemLog.create()
            debug(("msg", "http source sync"), ("flowName", flowName), ("params", params))
            onComplete(doScheduleSync(source, trace, flowName, params)) {
              result =>
                complete(
                  gson.toJson(result.get).parseJson
                )
            }
          }
        }
      },
      get {
        path("async" / Segment / Segment / Segment / Remaining) { (source, trace, time, flowName) =>
          parameterMap { params =>
            SystemLog.create()
            debug(("msg", "http source async"), ("flowName", flowName), ("params", params))
            onComplete(doScheduleAsync(source, trace, flowName, params)) {
              result =>
                complete(
                  gson.toJson(result.get).parseJson
                )
            }
          }
        }
      }
    )
  }

  def doScheduleSync(sourceName: String, trace: String, flowName: String, body: Any): Future[Value] = {
    val p = Promise[Value]()
    Future {
      val source = sources.get(sourceName).getOrElse(null)
      if (source != null) {
        try {
          //val instance = source.asInstanceOf[HttpSource].pushSync(trace, flowName, body)
          val syncEventBus = source.eventBus().asInstanceOf[SyncEventBus]
          val instance = syncEventBus.doProcessSync(httpEventBuilder(trace, flowName, body))
          p.success(Value(instance))
        } catch {
          case cause: Throwable =>
            p.failure(cause)
        }
      } else {
        p.failure(EventProcessException(SourceError))
      }
    }
    p.future
  }

  def doScheduleAsync(sourceName: String, trace: String, flowName: String, body: Any): Future[Value] = {
    val p = Promise[Value]()
    Future {
      val source = sources.get(sourceName).getOrElse(null)
      if (source != null) {
        try {
          val eventBus = source.eventBus()
          val events = eventBus.doProcess(List[FlowEvent](httpEventBuilder(trace, flowName, body)))
          //val events = source.asInstanceOf[HttpSource].pushAsync(trace, flowName, List[Any](body))
          val event = events(0)
          if (event == null) {
            p.failure(EventProcessException(FormatError))
          } else {
            p.success(Value(event))
          }
        } catch {
          case cause: Throwable =>
            p.failure(cause)
        }
      } else {
        p.failure(EventProcessException(SourceError))
      }
    }
    p.future
  }

  def httpEventBuilder(trace: String, flow: String, data: Any): FlowEvent = {
    val eventId = getEventId
    val traceId = trace match {
      case "" | "-" | "_" =>
        getTraceId(eventId)
      case _ =>
        trace
    }
    val flowName = s"/$flow"
    val builder = FlowEvent.newBuilder()
      .setEventId(eventId)
      .setFlowName(flowName)
      .setTraceId(traceId)
    data match {
      case json: String =>
        val value = gson.fromJson(json, classOf[Value])
        builder.putArgument("@", TraceVariable.newBuilder().setValue(value.as[FlowValue]).build()).build()
      case params: Map[String, String] =>
        params.foreach {
          case (key, json) =>
            val value = Try(gson.fromJson(json, classOf[Value])).getOrElse(Value(json))
            builder.putArgument(s"@$key", TraceVariable.newBuilder().setValue(value.as[FlowValue]).build())
        }
        builder.build()
      case _ =>
        builder.build()
    }
  }

}
