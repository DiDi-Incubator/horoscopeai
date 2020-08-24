/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: liangguorong@didiglobal.com
 */

package com.didichuxing.horoscope.logging.ods.impl

import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowInstance
import com.didichuxing.horoscope.logging.ods.OdsLogger

class CompositeOdsLogger(loggers: Seq[OdsLogger]) extends OdsLogger {
  override def log(flowInstance: FlowInstance): Unit = loggers.foreach(_.log(flowInstance))
}
