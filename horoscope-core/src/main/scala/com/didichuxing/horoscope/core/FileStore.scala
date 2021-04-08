package com.didichuxing.horoscope.core

import java.io.File
import akka.http.scaladsl.server.Route

trait FileStore {

  def getFile(path: String): File

  def listFiles(url: String): (String, Seq[File])

  def updateFile(path: String, content: String): Boolean

  def deleteFile(path: String): Boolean

  def createFile(path: String, isDirectory: Boolean): Boolean

  def copyFile(path: String): Boolean

  def renameFile(path: String, name: String): Boolean

  def api: Route = _.reject()
}
