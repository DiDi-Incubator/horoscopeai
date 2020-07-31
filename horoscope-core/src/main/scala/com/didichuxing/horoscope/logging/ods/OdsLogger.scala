/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: liangguorong@didiglobal.com
 */

package com.didichuxing.horoscope.logging.ods

import com.didichuxing.horoscope.core.FlowRuntimeMessage.{FlowInstance, FlowInstanceOrBuilder}

trait OdsLogger {
  def log(flowInstance: FlowInstance): Unit
}
