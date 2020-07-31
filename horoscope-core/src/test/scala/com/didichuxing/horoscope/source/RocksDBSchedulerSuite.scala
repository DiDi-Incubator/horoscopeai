/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.source

import akka.actor.ActorSystem
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.service.resource.SlotRange
import com.didichuxing.horoscope.service.scheduler.RocksDBScheduler
import com.didichuxing.horoscope.service.source.DefaultSourceExecutionContext
import com.didichuxing.horoscope.service.storage.HBaseTraceStore
import com.didichuxing.horoscope.util.Logging
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfter, FunSuite, Ignore}

@Ignore
class RocksDBSchedulerSuite extends FunSuite with BeforeAndAfter with Logging {

  var rocksDBScheduler: RocksDBScheduler = _
  var traceStore: HBaseTraceStore = _

  before {
    val system = ActorSystem("application-remoting-2552")
    val hconf = ConfigFactory.load("application-remoting-2552.conf")
    traceStore = new HBaseTraceStore()
    traceStore.start(hconf)
    implicit val ctx: ApplicationContext = ApplicationContext()
    ctx.withActorSystem(system)
    ctx.withConfig(hconf)
    ctx.withTraceStore(traceStore)
    ctx.withSourceExecutionContext(DefaultSourceExecutionContext(hconf))
    rocksDBScheduler = new RocksDBScheduler()
  }

  test("concurrent recovery") {
    rocksDBScheduler.concurrentGetEventsBySource(SlotRange(3, 11))
  }
}
