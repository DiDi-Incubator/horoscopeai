/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.storage

import com.didichuxing.horoscope.core.TraceStore
import com.typesafe.config.Config

abstract class AbstractTraceStore extends TraceStore {

  protected var config: Config = _

  def start(conf: Config): Unit = {
    config = conf
  }

  def stop(): Unit = {

  }

}
