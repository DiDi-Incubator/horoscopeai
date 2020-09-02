/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.core

import akka.http.scaladsl.server.Route
import com.didichuxing.horoscope.core.FlowRuntimeMessage._

import scala.concurrent.Future

/**
  * Below describes the data model of horoscope:
  *
  * trace: struct
  *  |-- mailbox: array
  *  |    |-- element: struct
  *  |    |    |-- source: string (nullable = true)
  *  |    |    |-- event: FlowEvent
  *  |-- context: map
  *  |    |-- key: string
  *  |    |-- value: FlowDependency
  *  |-- history: array
  *  |    |-- element: FlowInstance
  *
  * Everything happens in horoscope belongs to some trace, which is the nature primary key. Besides, there are
  * three entities needed to be persisted:
  *   * mailbox, for pending events from different sources. If horoscope restarts or recovers from failure,
  *     corresponding flows will be re-executed. If event is from "goto" statement, source is null.
  *     Some FlowEvent may contains 'token' field. For a given token value, at most one event at mailbox could
  *     be its owner at any time, and the ownership is decided at the time when event enters mailbox.
  *   * context, the current snapshot of available variables which can been seen by following flows on the same
  *     trace.
  *   * history, log of executed flow.
  *
  * On the top of storage model, we define some atomic actions with transaction grantees.
  */
trait TraceStore {
  /**
    * Add a new event to store, try to acquire and return token when needed
    */
  def addEvent(source: String, event: FlowEvent.Builder): FlowEventOrBuilder

  /**
   * Batch add new events to store
   */
  def addEvents(source: String, events: List[FlowEvent.Builder]): List[FlowEventOrBuilder]

  /**
    * Extract information from instance, and must perform three steps in transaction:
    * 1. save all $variable updates to context
    * 2. if next event exists, put it to mailbox, with source set to null
    * 3. remove event from mailbox
    *
    * Besides, store can save instance to history for analyzing purpose
  */
  def commitEvent(source: String, instance: FlowInstance.Builder): FlowInstanceOrBuilder

  /**
    * Get all pending events from source
    */
  def getEventsBySource(source: String, beginSlot: Int, endSlot: Int): Iterable[FlowEventOrBuilder]

  /**
    * Get most recent snapshot of trace context
    */
  def getContext(trace: String, keys: Array[String]): Future[Map[String, TraceVariableOrBuilder]]

  /**
   * Poll scheduler event
   */
  def pollSchedulerEvents(source: String, slot: Int, timestamp: Long, limit: Int): Iterable[FlowEvent]

  /**
   * Commit success process scheduler event
   */
  def commitSchedulerEvents(source: String, slot: Int, events: List[FlowEvent]): Long

  def api: Route = _.reject()
}
