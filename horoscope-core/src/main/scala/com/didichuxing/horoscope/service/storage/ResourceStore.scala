package com.didichuxing.horoscope.service.storage

import java.nio.file.Paths

import com.didichuxing.horoscope.core.FileStore
import com.didichuxing.horoscope.util.Logging
import org.apache.commons.io.FileUtils

class ResourceStore(fileStore: FileStore, url: String = "") extends Logging {
  var resources: Map[String, Array[Byte]] = Map.empty
  private val (prefix, files) = fileStore.listFiles(url)

  load()

  def load(): Unit = {
    resources = files.filterNot(
      f => f.getAbsolutePath.endsWith(".flow") || f.getAbsolutePath.endsWith(".py")
    ).map { file =>
      info(("msg", "load resource"), ("name", file.getName), ("absolutePath", file.getAbsolutePath))
      file.getAbsolutePath -> FileUtils.readFileToByteArray(file)
    }.toMap
  }

  // absolute path starts with /, relative path is resolved against from the given namespace
  def get(namespace: String)(path: String): Array[Byte] = {
    var realPath = ""
    try {
      realPath = if (path.startsWith("/")) {
        Paths.get(prefix, path).toRealPath().toString
      } else {
        Paths.get(prefix, namespace).resolve(path).toRealPath().toString
      }
      resources(realPath)
    } catch {
      case e: Throwable =>
        error(("msg", "get resource error"), ("namespace", namespace), ("path", path), ("realpath", realPath))
        throw e
    }
  }
}
