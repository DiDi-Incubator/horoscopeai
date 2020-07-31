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
import com.google.common.io.Resources
import org.antlr.v4.runtime.CharStreams
import scala.collection.JavaConverters._

/**
 * base json file flow store
 * resource/infoflow/.flow
 */
class DefaultFlowStore extends FileFlowStore with Logging {

  def loadFileList(): Seq[File] = {
    val rootPath = Resources.getResource("infoflow/v2").getFile
    val root = new File(rootPath)
    root.listFiles().filter(_.isFile).toSeq
  }

}
