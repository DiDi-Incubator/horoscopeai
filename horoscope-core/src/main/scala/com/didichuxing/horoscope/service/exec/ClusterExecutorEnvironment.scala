/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.exec

import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.util.Utils
import com.didichuxing.horoscope.util.Utils.getSlot


class ClusterExecutorEnvironment(implicit ctx: ApplicationContext) extends LocalExecutorEnvironment {

  implicit val resourceManager = ctx.resourceManager
  val slotCount = Utils.getClusterSlotCount(ctx.config)

  override def shouldAccept(traceId: String): Boolean = {
    val localServer = resourceManager.local()
    val slotRange = resourceManager.getSlotRange(localServer)
    if (slotRange.isDefined) {
      val range = slotRange.get
      val slot = getSlot(traceId, slotCount)
      if (slot >= range.begin && slot < range.end) {
        true
      } else {
        false
      }
    } else {
      false
    }
  }
}
