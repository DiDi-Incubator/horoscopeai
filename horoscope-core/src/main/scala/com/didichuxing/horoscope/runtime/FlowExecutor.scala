/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.runtime

import com.didichuxing.horoscope.core.FlowRuntimeMessage.{FlowEvent, FlowInstance}

import scala.concurrent.Future

trait FlowExecutor {
  def start()

  def stop()

  def execute(event: FlowEvent): Future[FlowInstance]
}
