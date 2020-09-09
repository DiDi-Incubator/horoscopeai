/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.core

import com.typesafe.config.Config

trait Source {
  def start(eventBus: EventBus): Unit

  def stop(): Unit

  def eventBus(): EventBus
}

trait SourceFactory extends Serializable {
  def newSource(config: Config): Source
}
