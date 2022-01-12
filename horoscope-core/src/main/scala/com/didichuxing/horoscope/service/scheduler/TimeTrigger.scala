/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.scheduler

import java.util.{Timer, TimerTask}
import com.didichuxing.horoscope.util.{Logging, Utils}
import com.typesafe.config.Config

@deprecated(since = "0.6.3")
trait TimeTrigger {

  def start(scheduler: Scheduler)

  def stop()

}

@deprecated(since = "0.6.3")
class DefaultTimeTrigger(config: Config) extends TimeTrigger with Logging {

  val triggerTime = Utils.configIntOrDefault(config, "horoscope.scheduler.trigger", 60)
  val timer = new Timer()

  override def start(scheduler: Scheduler): Unit = {
    timer.schedule(new TimerTask {
      override def run(): Unit = {
        scheduler.onTimeSpanSwitch(System.currentTimeMillis())
      }
    }, 1000 * triggerTime, 1000 * triggerTime)
  }

  override def stop(): Unit = {
    timer.cancel()
  }

}
