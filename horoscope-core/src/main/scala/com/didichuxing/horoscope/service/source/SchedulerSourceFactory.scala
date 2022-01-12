/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import com.didichuxing.horoscope.core.Sources.EventBuilder
import com.didichuxing.horoscope.core.{Source, SourceFactory}
import com.didichuxing.horoscope.runtime.Value
import com.typesafe.config.Config

@deprecated(since = "0.6.3")
class SchedulerSourceFactory(builder: EventBuilder[List[Array[Byte]], List[Value]]) extends SourceFactory {

  override def newSource(config: Config): Source = {
    new SchedulerSource(config, builder)
  }
}
