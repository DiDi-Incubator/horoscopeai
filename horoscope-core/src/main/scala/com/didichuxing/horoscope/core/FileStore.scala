package com.didichuxing.horoscope.core

import java.io.File

import akka.http.scaladsl.server.Route

trait FileStore {

  def getFile(path: String): File

  /**
   * List files under path recursively
   */
  def listFiles(path: String): Seq[File]

  def updateFile(path: String, content: String): Boolean

  def deleteFile(path: String): Boolean

  def api: Route = _.reject()
}
