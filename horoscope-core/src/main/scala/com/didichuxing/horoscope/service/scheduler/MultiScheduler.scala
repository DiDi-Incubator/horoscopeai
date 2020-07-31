/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.scheduler

import com.didichuxing.horoscope.core.EventBus
import com.typesafe.config.Config

trait MultiScheduler {

  def start(source: String, params: Config, eventBus: EventBus)

  def stop(source: String)

}
