/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: liangguorong@didiglobal.com
 */

package com.didichuxing.horoscope.core

import akka.http.scaladsl.server.Route
import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowInstance

trait OdsLogger {
  def log(flowInstance: FlowInstance): Unit = throw new NotImplementedError()

  def api: Route = _.reject()
}
