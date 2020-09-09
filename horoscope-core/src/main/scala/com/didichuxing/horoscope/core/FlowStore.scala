/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.core

import akka.http.scaladsl.server.Route
import com.didichuxing.horoscope.core.FlowDslMessage.FlowDef

trait FlowStore {
  def getFlowByName(name: String): FlowDef

  def api: Route = _.reject()
}
