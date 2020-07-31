/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import com.didichuxing.horoscope.runtime.{Implicits, Value}
import com.didichuxing.horoscope.service.cluster.FlowClient

import scala.concurrent.Future
import scala.util.Try
import Implicits.gson
import com.typesafe.config.{Config, ConfigFactory}

class SourceControl(client: FlowClient)(implicit config: Config, system: ActorSystem) {

  val port = Try(config.getInt("horoscope.source.http.port")).getOrElse(5880)

  case class Command(command: String, source: String, remote: Boolean = true, config: String = "")

  def startSource(sourceName: String, remote: Boolean): Unit = {
    val local = client.getLocal()
    val parts = client.getParticipants()
    for(part <- parts) {
      val host = part.getHost()
      if(host.equals(local.getHost())) {
        client.startSource(sourceName)
      } else {
        if(remote) {
          val url = s"http://$host:$port/sourceCtrl"
          postEvent(url, Command("start", sourceName, false))
        }
      }
    }
  }

  def stopSource(sourceName: String, remote: Boolean): Unit = {
    val local = client.getLocal()
    val parts = client.getParticipants()
    for(part <- parts) {
      val host = part.getHost()
      if(host.equals(local.getHost())) {
        client.stopSource(sourceName)
      } else {
        if(remote) {
          val url = s"http://$host:$port/sourceCtrl"
          postEvent(url, Command("stop", sourceName, false))
        }
      }
    }
  }

  def removeSource(sourceName: String, remote: Boolean): Unit = {
    val local = client.getLocal()
    val parts = client.getParticipants()
    for(part <- parts) {
      val host = part.getHost()
      if(host.equals(local.getHost())) {
        client.removeSource(sourceName)
      } else {
        if(remote) {
          val url = s"http://$host:$port/sourceCtrl"
          postEvent(url, Command("remove", sourceName, false))
        }
      }
    }
  }

  def listAllRunning(): Seq[String] = {
    client.listAllRunning()
  }

  def listAllRegistered(): Seq[String] = {
    client.listAllRegistered()
  }


  def registerSource(sourceConfig: String, remote: Boolean): Unit = {
    val local = client.getLocal()
    val parts = client.getParticipants()
    for(part <- parts) {
      val host = part.getHost()
      if(host.equals(local.getHost())) {
        client.registerSource(ConfigFactory.parseString(sourceConfig))
      } else {
        if(remote) {
          val url = s"http://$host:$port/sourceCtrl"
          postEvent(url, Command("register", "", false, sourceConfig))
        }
      }
    }
  }

  private def postEvent(url: String, command: Command): Future[HttpResponse] = {
    val event: Value = Value(command)
    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = url,
      entity = HttpEntity(ContentTypes.`application/json`, event.toJson)
    )
    Http().singleRequest(request)
  }
}
