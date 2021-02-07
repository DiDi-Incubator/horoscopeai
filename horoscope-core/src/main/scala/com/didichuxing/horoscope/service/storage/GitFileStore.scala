package com.didichuxing.horoscope.service.storage

import java.io.File

import com.didichuxing.horoscope.core.FileStore
import com.didichuxing.horoscope.util.Logging

class GitFileStore extends FileStore with Logging {
  override def getFile(path: String): File = ???

  /**
   * List files under path recursively
   */
  override def listFiles(path: String): Seq[File] = ???

  override def updateFile(path: String, content: String): Boolean = ???

  override def deleteFile(path: String): Boolean = ???
}
