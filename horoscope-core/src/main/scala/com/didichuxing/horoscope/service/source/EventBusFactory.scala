/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import com.didichuxing.horoscope.core.EventBus
import com.didichuxing.horoscope.service.ApplicationContext
import com.typesafe.config.Config

object EventBusFactory {
  def newEventBus(sourceName: String, flowName: String, params: Config)
                 (implicit ctx: ApplicationContext): EventBus = new DefaultEventBus(sourceName, flowName, params)
}
