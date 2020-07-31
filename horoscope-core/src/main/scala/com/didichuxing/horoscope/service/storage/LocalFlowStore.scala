/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.storage

import java.io.File

import com.didichuxing.horoscope.core.FlowDslMessage.FlowDef
import com.didichuxing.horoscope.core.FlowStore
import com.didichuxing.horoscope.dsl.FlowCompiler
import com.didichuxing.horoscope.util.Logging
import org.antlr.v4.runtime.CharStreams

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
 * subdirectories flow store
 * resource/infoflow/../.flow
 */
class LocalFlowStore extends FileFlowStore with Logging {

  override def loadFileList(): Seq[File] = {
    val ccl = Thread.currentThread.getContextClassLoader
    val urls = ccl.getResources("infoflow").toList
    val roots = urls.map(url => new File(url.getFile))
    roots.flatMap(flowFiles(_))
  }

  /**
   * 遍历子目录
   */
  private def flowFiles(f: File): Seq[File] = {
    val files = ListBuffer[File]()
    f.listFiles().foreach(child => {
      if (child.isDirectory()) {
        files.appendAll(flowFiles(child))
      } else if (child.isFile()) {
        files.append(child)
      }
    })
    files.toList
  }


}
