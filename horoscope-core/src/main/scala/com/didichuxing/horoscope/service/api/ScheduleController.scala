package com.didichuxing.horoscope.service.api

import akka.http.scaladsl.server.Route
import com.didichuxing.horoscope.core.Source
import com.didichuxing.horoscope.runtime.Implicits.gson
import com.didichuxing.horoscope.service.source.EventProcessErrorCode._
import com.didichuxing.horoscope.service.source.{EventProcessException, HttpSource}
import com.didichuxing.horoscope.util.{Logging, SystemLog}
import com.didichuxing.horoscope.runtime.Value

import scala.collection.mutable
import scala.concurrent.{ExecutionContextExecutorService, Future, Promise}

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
          entity(as[String]) { json =>
            SystemLog.create()
            debug(("msg", "http source sync"), ("flowName", flowName), ("body", json))
            onComplete(doSchedule(source, trace, flowName, json)) {
              result =>
                complete(
                  gson.toJson(result.get).parseJson
                )
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
            onComplete(doSchedule(source, trace, flowName, params)) {
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

  def doSchedule(sourceName: String, trace: String, flowName: String, body: Any): Future[Value] = {
    val p = Promise[Value]()
    Future {
      val source = sources.get(sourceName).getOrElse(null)
      if (source != null && source.isInstanceOf[HttpSource]) {
        try {
          val instance = source.asInstanceOf[HttpSource]
            .pushSync(trace, flowName, body)
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
      if (source != null && source.isInstanceOf[HttpSource]) {
        try {
          val events = source.asInstanceOf[HttpSource]
            .pushAsync(trace, flowName, List[Any](body))
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

}
