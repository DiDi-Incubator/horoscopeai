/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.storage

import com.didichuxing.horoscope.core.FlowRuntimeMessage._
import com.didichuxing.horoscope.core.FlowRuntimeMessage
import com.didichuxing.horoscope.util.Constants._
import com.didichuxing.horoscope.util.Utils._
import com.didichuxing.horoscope.util.Logging

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.collection.JavaConversions._
import scala.util.Try

//eventStores: source: eventList
//traceContext: valueName: value
case class TraceStoreEntity(eventStores: mutable.Map[String, ListBuffer[FlowEvent]],
                            traceContext: mutable.Map[String, TraceVariable])

class DefaultTraceStore extends AbstractTraceStore with Logging {

  //key: traceId
  val traceStores = mutable.Map[String, TraceStoreEntity]()
  val orderByTimestamp = Ordering.by[FlowEvent, Long](_.getScheduledTimestamp)
  val multiScheduler = mutable.Map[String, mutable.SortedSet[FlowEvent]]()

  /**
   * Add a new event to store, try to acquire and return token when needed
   */
  override def addEvent(source: String, event: FlowEvent.Builder): FlowRuntimeMessage.FlowEventOrBuilder = {
    val traceId = event.getTraceId
    val tse = traceStores.get(traceId)
      .getOrElse(TraceStoreEntity(mutable.Map[String, ListBuffer[FlowEvent]](), mutable.Map[String, TraceVariable]()))
    traceStores.put(traceId, tse)
    val flowEvent = event.build()
    val mailbox = tse.eventStores.get(source).getOrElse(ListBuffer[FlowEvent]())
    tse.eventStores.put(source, mailbox)
    mailbox.append(flowEvent)
    flowEvent
  }

  override def addEvents(source: String, events: List[FlowEvent.Builder]): List[FlowEventOrBuilder] = {
    for (event <- events) {
      val traceId = event.getTraceId
      val tse = traceStores.get(traceId)
        .getOrElse(TraceStoreEntity(mutable.Map[String, ListBuffer[FlowEvent]](), mutable.Map[String, TraceVariable]()))
      traceStores.put(traceId, tse)
      val flowEvent = event.build()
      val mailbox = tse.eventStores.get(source).getOrElse(ListBuffer[FlowEvent]())
      tse.eventStores.put(source, mailbox)
      mailbox.append(flowEvent)
    }
    events
  }

  /**
   * Extract information from instance, and must perform three steps in transaction:
   * 1. save all $variable updates to context
   * 2. if next event exists, put it to mailbox, with source set to null
   * 3. remove event from mailbox
   *
   * Besides, store can save instance to history for analyzing purpose
   */
  override def commitEvent(source: String, instance: FlowInstance.Builder): FlowRuntimeMessage.FlowInstanceOrBuilder = {
    val event = instance.getEvent
    val traceId = event.getTraceId
    val tse = traceStores.get(traceId).get
    val mailbox = tse.eventStores.get(source).get
    val context = tse.traceContext
    //context
    //v2
    instance.getUpdateList.foreach(v => {
      val name = v.getReference.getName
      context.put(name, v)
    })
    //delete mailbox
    val delEvent = mailbox.filter(event => event.getEventId == instance.getEvent.getEventId)
    mailbox --= delEvent
    //multi scheduler
    val events = multiScheduler.getOrElseUpdate(source, mutable.SortedSet[FlowEvent]()(orderByTimestamp))
    instance.getScheduleList().foreach(event => {
      events.add(event)
    })
    instance.build()
  }

  /**
   * Get all pending events from source
   */
  override def getEventsBySource(source: String, beginSlot: Int, endSlot: Int):
  Iterable[FlowRuntimeMessage.FlowEventOrBuilder] = {
    val flowEvents = ListBuffer[FlowRuntimeMessage.FlowEventOrBuilder]()
    for (trace <- traceStores.values) {
      val mailbox = trace.eventStores
      if (mailbox.get(source).isDefined) {
        val events = mailbox.get(source).get.toList
        flowEvents.appendAll(events)
      }
    }
    flowEvents
  }

  /**
   * Get most recent snapshot of trace context
   */
  override def getContext(trace: String,
                          keys: Array[String]): Future[Map[String, TraceVariableOrBuilder]] = {
    var contexts = Map[String, TraceVariableOrBuilder]()
    for (key <- keys) {
      val value = Try(traceStores.get(trace).get.traceContext.get(key).get)
      if (value.isSuccess) {
        contexts += (key -> value.get)
      } else {
        warn(s"Context key ${key} not found")
      }
    }
    Future.successful(contexts)
  }

  /**
   * Get schedule event
   */
  override def pollSchedulerEvents(source: String, slot: Int, timestamp: Long, limit: Int): Iterable[FlowEvent] = {
    val events = multiScheduler.get(source)
    if (events.isEmpty) {
      List[FlowEvent]()
    } else {
      val untilEvent = FlowEvent.getDefaultInstance().toBuilder()
        .setEventId("")
        .setTraceId("")
        .setFlowName("")
        .setScheduledTimestamp(timestamp).build()
      events.get.until(untilEvent)
    }
  }

  /**
   * Commit success process scheduler event
   */
  override def commitSchedulerEvents(source: String, slot: Int, events: List[FlowEvent]): Long = {
    if (events.size == 0) {
      0
    } else {
      multiScheduler.get(source).get.removeAll(events)
      events.size
    }
  }
}
