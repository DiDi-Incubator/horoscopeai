package com.didichuxing.horoscope.core

import akka.http.scaladsl.server.Route
import com.typesafe.config.Config

trait SourceStore {

  def putSource(source: Config)

  def removeSource(sourceName: String)

  def getSources(): Seq[Config]

  def getSource(sourceName: String): Config

  def api: Route = _.reject()

}
