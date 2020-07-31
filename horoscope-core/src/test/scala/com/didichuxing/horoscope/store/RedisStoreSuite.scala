/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.store

import com.didichuxing.horoscope.core.FlowRuntimeMessage.{FlowEvent, FlowInstance, FlowValue}
import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowEvent.TokenStatus
import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowInstance.Assign
import com.didichuxing.horoscope.service.storage.RedisTraceStore
import com.didichuxing.horoscope.util.{Logging, Utils}
import com.didichuxing.horoscope.util.Utils.schStoreCol
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfter, FunSuite, Ignore}

@Ignore
class RedisStoreSuite extends FunSuite with BeforeAndAfter with Logging {

  /*val pool = new JedisPool()

  test("hash set") {
    val c = pool.getResource
    val m = Map[String, String]("b" -> "3")
    println(c.hmset("hash_demo", m))
    c.expire("hash_demo", 1)
    println(c.hgetAll("hash_demo"))
    Thread.sleep(2000)
    println(c.hgetAll("hash_demo"))
    c.close()

    pool.close()
  }

  test("sort set") {
    val c = pool.getResource
    c.zadd("sort_demo", 10000, "a")
    c.zadd("sort_demo", 10002, "b")
    c.zadd("sort_demo", 10003, "c")
    c.zadd("sort_demo", 10006, "d")
    c.zadd("sort_demo", 10001, "e")

    println(c.zrangeByScore("sort_demo", 0, 10002))
    println(c.zrevrangeByScore("sort_demo", 0, 10002))
    pool.close()
  }*/

  var traceStore: RedisTraceStore = _
  var hconf: Config = _

  before {
    hconf = ConfigFactory.load("application-remoting-2552.conf")
    traceStore = new RedisTraceStore()
    traceStore.start(hconf)
  }

  test("trace context v1") {
    val flowName = "/root/flow1"
    val gotoFlowName = "/root/flow-goto-demo"
    val source = "source1"
    val eventId = Utils.getEventId()
    val traceId = Utils.getTraceId(eventId)
    val event = FlowEvent.newBuilder()
      .setEventId(eventId)
      .setTraceId(traceId)
      .setFlowName(flowName)
    //1. add event 1
    val flowEvent = traceStore.addEvent(source, event)
    info(("add event", flowEvent.getEventId))

    //2. execute event1
    val instance = FlowInstance.newBuilder()
      .setFlowId("")
      .setEvent(event)
      .setGoto(createDelayGotoEvent(traceId, gotoFlowName))
      .addAssign(Assign.newBuilder().setName("$test").setValue(FlowValue.newBuilder().setText("hello")))

    //3. commit event 1(source)
    val flowInstance = traceStore.commitEvent(source, instance)
    info(("commit event 1", flowInstance.getEvent.getEventId))

    //4. execute event 2
    val instance2 = FlowInstance.newBuilder()
      .setFlowId("")
      .setEvent(flowInstance.getGoto)
      .setGoto(createDelayGotoEvent(traceId, gotoFlowName))
      .addAssign(Assign.newBuilder().setName("$test2").setValue(FlowValue.newBuilder().setText("hello2")))

    //5. commit event 2(goto)
    val flowInstance2 = traceStore.commitEvent(schStoreCol(hconf), instance2)
    info(("commit event 2", flowInstance2.getEvent.getEventId))

    //6. execute event 3
    val instance3 = FlowInstance.newBuilder()
      .setFlowId("")
      .setEvent(flowInstance2.getGoto)
      .addAssign(Assign.newBuilder().setName("$test").setValue(FlowValue.newBuilder().setText("hello2")))

    //7. commit event 3
    val flowInstance3 = traceStore.commitEvent(schStoreCol(hconf), instance3)
    info(("commit event 3", flowInstance3.getEvent.getEventId))
  }

  test("trace context v2") {
    val flowName = "/root/flow1"
    val gotoFlowName = "/root/flow-goto-demo"
    val source = "source1"
    val eventId = Utils.getEventId()
    val traceId = Utils.getTraceId(eventId)
    val event = FlowEvent.newBuilder()
      .setEventId(eventId)
      .setTraceId(traceId)
      .setFlowName(flowName)
    //1. add event 1
    val flowEvent = traceStore.addEvent(source, event)
    info(("add event", flowEvent.getEventId))

    //2. execute event1
    val instance = FlowInstance.newBuilder()
      .setFlowId("")
      .setEvent(event)
      .setGoto(createGotoEvent(traceId, gotoFlowName))
      .addAssign(Assign.newBuilder().setName("$test").setValue(FlowValue.newBuilder().setText("hello")))
      .addSchedule(createSchedulerEvent(gotoFlowName, 0))
      .addSchedule(createSchedulerEvent(gotoFlowName, 0))

    //3. commit event 1(source)
    val flowInstance = traceStore.commitEvent(source, instance)
    info(("commit event 1", flowInstance.getEvent.getEventId))

    //4. execute event 2
    val instance2 = FlowInstance.newBuilder()
      .setFlowId("")
      .setEvent(flowInstance.getGoto)
      .setGoto(createGotoEvent(traceId, gotoFlowName))
      .addAssign(Assign.newBuilder().setName("$test2").setValue(FlowValue.newBuilder().setText("hello2")))
      .addSchedule(createSchedulerEvent(gotoFlowName, System.currentTimeMillis()))

    //5. commit event 2(goto)
    val flowInstance2 = traceStore.commitEvent(source, instance2)
    info(("commit event 2", flowInstance2.getEvent.getEventId))

    //6. execute event 3
    val instance3 = FlowInstance.newBuilder()
      .setFlowId("")
      .setEvent(flowInstance2.getGoto)
      .addAssign(Assign.newBuilder().setName("$test").setValue(FlowValue.newBuilder().setText("hello2")))

    //7. commit event 3
    val flowInstance3 = traceStore.commitEvent(source, instance3)
    info(("commit event 3", flowInstance3.getEvent.getEventId))
  }

  private def createGotoEvent(traceId: String, flowName: String): FlowEvent = {
    FlowEvent.newBuilder()
      .setEventId(Utils.getEventId())
      .setTraceId(traceId)
      .setFlowName(flowName)
      //.setScheduledTimestamp(System.currentTimeMillis())
      .setToken(TokenStatus.newBuilder().setName("linkId").setValue("link0001").setOwner(""))
      .build()
  }

  private def createDelayGotoEvent(traceId: String, flowName: String): FlowEvent = {
    FlowEvent.newBuilder()
      .setEventId(Utils.getEventId())
      .setTraceId(traceId)
      .setFlowName(flowName)
      .setScheduledTimestamp(System.currentTimeMillis())
      .setToken(TokenStatus.newBuilder().setName("linkId").setValue("link0001").setOwner(""))
      .build()
  }

  private def createSchedulerEvent(flowName: String, timestamp: Long): FlowEvent = {
    val eventId = Utils.getEventId()
    FlowEvent.newBuilder()
      .setEventId(eventId)
      .setTraceId(Utils.getTraceId(eventId))
      .setFlowName(flowName)
      .setScheduledTimestamp(timestamp)
      .build()
  }

  test("source recovery") {
    val events = traceStore.getEventsBySource("_SCD_", 0, 1024)
    events.foreach(e => println(e.getEventId))
  }

  test("scheduler poll") {
    val events = traceStore.pollSchedulerEvents("source1", 634, System.currentTimeMillis(), 10)
    events.foreach(e => println(e.getEventId))
  }
}
