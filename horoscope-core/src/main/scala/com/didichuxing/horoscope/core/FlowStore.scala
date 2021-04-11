/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.core

import akka.http.scaladsl.server.Route
import com.didichuxing.horoscope.runtime.experiment.ExperimentController
import com.didichuxing.horoscope.runtime.expression.BuiltIn

trait FlowStore {
  def getFlow(name: String): Flow = throw new NotImplementedError()

  def getController(name: String): Option[ExperimentController] = throw new NotImplementedError()

  def getBuiltIn: BuiltIn

  def api: Route = _.reject()
}
