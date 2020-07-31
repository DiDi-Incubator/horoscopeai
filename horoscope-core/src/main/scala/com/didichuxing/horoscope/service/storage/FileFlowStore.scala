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

abstract class FileFlowStore extends FlowStore with Logging {

  protected val fileList: Seq[File] = loadFileList()
  protected val flowList: Map[String, FlowDef] = loadFlowList()

  def loadFileList(): Seq[File]

  def loadFlowList(): Map[String, FlowDef] = {
    val flows = fileList
      .flatMap { file => {
        try {
          val charStream = CharStreams.fromPath(file.toPath)
          val compiler = new FlowCompiler()
          val flow: FlowDef = compiler.compile(charStream)
          info(("msg", s"load flow def ${file.getName}"))
          Some(flow.getName -> flow)
        } catch {
          case e: Throwable =>
            e.printStackTrace()
            logError(("flow", s"${file.getAbsolutePath}"), ("ex", e.toString))
            None
        }
      }
      }
    val m = flows.toMap
    assert(flows.size == m.size, s"flow store has duplicated flow name: " +
      s"${flows.groupBy(_._1).filter(_._2.size > 1).map(_._1).toSeq.sorted.mkString(",")}")
    m
  }

  override def getFlowByName(name: String): Option[FlowDef] = {
    flowList.get(name)
  }
}
