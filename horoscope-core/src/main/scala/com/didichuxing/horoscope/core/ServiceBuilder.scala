/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.core

import akka.actor.ActorSystem
import com.didichuxing.horoscope.core.OdsLogger
import com.didichuxing.horoscope.runtime.expression.BuiltIn
import com.didichuxing.horoscope.service.source.SourceExecutionContext
import com.typesafe.config.Config

trait ServiceBuilder {

  def withConfig(config: Config): this.type

  def withFlowStore(flowStore: FlowStore): this.type

  def withFileStore(fileStore: FileStore): this.type

  def withConfigStore(configStore: ConfigStore): this.type

  def withTraceStore(traceStore: TraceStore): this.type

  def withSourceFactory(name: String, factory: SourceFactory): this.type

  def withActorSystem(system: ActorSystem): this.type

  def withBuiltIn(builtIn: BuiltIn): this.type

  def withExecutionContext(executor: SourceExecutionContext): this.type

  def withOdsLogger(odsLogger: OdsLogger): this.type

}

