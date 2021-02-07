package com.didichuxing.horoscope.service.storage

import com.didichuxing.horoscope.core.{ConfigChangeListener, ConfigStore, FileStore, FlowDslMessage, FlowStore}
import com.didichuxing.horoscope.runtime.expression.{BuiltIn, DefaultBuiltIn, LocalJythonBuiltInV2}
import com.didichuxing.horoscope.util.Logging

class GitFlowStore(
  configStore: ConfigStore,
  fileStore: FileStore,
  extBuiltIn: Option[BuiltIn] = None) extends FlowStore with ConfigChangeListener with Logging {
  configStore.register(this)
  implicit val builtIn = {
    if (extBuiltIn.isDefined) {
      new LocalJythonBuiltInV2(fileStore).mergeFrom(extBuiltIn.get).mergeFrom(DefaultBuiltIn.defaultBuiltin)
    } else {
      new LocalJythonBuiltInV2(fileStore).mergeFrom(DefaultBuiltIn.defaultBuiltin)
    }
  }

  private val files = fileStore.listFiles("./")
  private val flows = files.filter(_.getAbsolutePath.endsWith(".flow"))
  private val graphQL = files.filter(_.getAbsolutePath.endsWith(".graphql"))

  override def getFlowByName(name: String): FlowDslMessage.FlowDef = ???

  override def onConfUpdate(): Unit = ???
}
