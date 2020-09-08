/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: liangguorong@didiglobal.com
 */

package com.didichuxing.horoscope.logging.ods.impl

import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowInstance
import com.didichuxing.horoscope.core.OdsLogger
import com.didichuxing.horoscope.util.PublicLog

class LocalOdsLogger(publicLog: PublicLog) extends OdsLogger with LogHelper {
  override def log(flowInstance: FlowInstance): Unit = {
    publicLog.public(("instance_simple", flowInstance.toJson()))
  }
}
