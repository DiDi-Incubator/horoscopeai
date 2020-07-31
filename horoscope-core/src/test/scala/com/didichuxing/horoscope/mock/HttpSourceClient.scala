/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.mock

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest, HttpResponse}
import akka.stream.ActorMaterializer
import com.google.gson.GsonBuilder

import scala.concurrent.{Await, Future}

case class Order(id: Int, data: String)

object HttpSourceClient {

  private val gson = new GsonBuilder().create()
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  def postEvent(url: String): HttpResponse = {
    val json = gson.toJson(Order(1, "abc"))
    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = url,
      entity = HttpEntity(ContentTypes.`application/json`, json)
    )
    val responseFuture: Future[HttpResponse] = Http().singleRequest(request)
    import scala.concurrent.duration._
    val response = Await.result(responseFuture, 10 seconds)
    println(response)
    response
  }

  def stop(): Unit = {
    system.terminate()
  }
}
