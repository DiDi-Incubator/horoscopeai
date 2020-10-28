/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.scheduler

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Function

import com.didichuxing.horoscope.core.EventBus
import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowEvent
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.service.resource.SlotRange
import com.didichuxing.horoscope.util.Utils.getClusterSlotCount
import com.didichuxing.horoscope.util.{Logging, SystemLog}
import com.typesafe.config.Config

import scala.collection.mutable.ListBuffer
import scala.math._
import scala.util.Try

/**
 * source启动后注册eventBus，
 *
 * @param ctx
 */
class DefaultMultiScheduler(implicit ctx: ApplicationContext) extends MultiScheduler with Logging {

  val ec = ctx.sourceExecutionContext.getExecutionContext()
  val traceStore = ctx.traceStore
  val runStatus = new ConcurrentHashMap[String, AtomicBoolean]()
  val slotCount = getClusterSlotCount(ctx.config)

  override def start(source: String, params: Config, eventBus: EventBus): Unit = {
    val schEnable = Try(params.getBoolean("scheduler.enable")).getOrElse(false)
    if (schEnable) {
      val resourceManager = ctx.resourceManager
      val isRun = getRunStatus(source)
      if (isRun.compareAndSet(false, true)) {
        ec.execute(new Runnable {
          override def run(): Unit = {
            SystemLog.create()
            while (isRun.get()) {
              try {
                val backpress = Try(params.getInt("backpress.permits")).getOrElse(100)
                val limit = min(10, max(1, backpress / 5))
                val serverCount = if (resourceManager == null) 1 else resourceManager.getParticipants().size
                val slotRange = if (resourceManager == null) {
                  debug(("msg", "local mode"))
                  Some(SlotRange(0, 1))
                } else {
                  resourceManager.getSlotRange(resourceManager.local())
                }
                if (slotRange.isDefined) {
                  val timestamp = System.currentTimeMillis()
                  var commitCount: Long = 0
                  for (slot <- slotRange.get.begin until slotRange.get.end) {
                    if (isRun.get()) {
                      val successEvents = poll(source, eventBus, slot, timestamp, limit)
                      val count = traceStore.commitSchedulerEvents(source, slot, successEvents)
                      if (count != successEvents.size) {
                        error(("msg", "multi scheduler commit error"),
                          ("success size", successEvents.size), ("commit size", count))
                      }
                      commitCount += count
                    }
                  }
                  val endTime = System.currentTimeMillis()
                  val procTime = endTime - timestamp
                  info(("msg", "multi scheduler poll commit"), ("source", source), ("slotRange", slotRange),
                    ("timestamp", timestamp), ("limit", limit), ("count", commitCount), ("proc_time", s"${procTime}ms"))
                  val factor = Try(params.getInt("scheduler.factor")).getOrElse(3)
                  val interval = slotCount / serverCount / factor
                  if (procTime < interval) {
                    Thread.sleep(interval - procTime)
                  }
                } else {
                  error(("msg", "multi scheduler slot range error"), ("source", source))
                }
              } catch {
                case iex: InterruptedException =>
                  stop(source)
                case ex: Throwable =>
                  error(("msg", "multi scheduler exception"), ("ex", ex.getMessage))
              }
            }
            info(("msg", "multi scheduler stop"), ("source", source))
          }
        })
      } else {
        error(("msg", "multi scheduler is running"), ("source", source))
      }
    }
  }

  override def stop(source: String): Unit = {
    getRunStatus(source).compareAndSet(true, false)
  }

  private def poll(name: String, eventBus: EventBus, slot: Int, timestamp: Long, max: Int): List[FlowEvent] = {
    val successEvents = ListBuffer[FlowEvent]()
    try {
      val startTime = System.currentTimeMillis()
      val events = traceStore.pollSchedulerEvents(name, slot, timestamp, max)
      if (events.size > 0) {
        val successes = eventBus.doProcess(events.toList)
        successEvents.appendAll(successes.filter(e => e != null))
      }
      val endTime = System.currentTimeMillis()
      if (events.size != successEvents.size) {
        error(("msg", "multi scheduler commit error"),
          ("poll size", events.size), ("process size", successEvents.size))
      }
      debug(("msg", "multi scheduler poll success"), ("source", name), ("slot", slot), ("timestamp", timestamp),
        ("limit", max), ("count", events.size), ("proc_time", s"${endTime - startTime}ms"))
      successEvents.toList
    } catch {
      case ex: Throwable =>
        error(("msg", "multi scheduler error"), ("source", name), ("slot", slot), ("timestamp", timestamp),
          ("ex", ex.getMessage))
        successEvents.toList
    }
  }

  private def getRunStatus(source: String): AtomicBoolean = {
    runStatus.computeIfAbsent(source, new Function[String, AtomicBoolean]() {
      override def apply(t: String): AtomicBoolean = {
        new AtomicBoolean(false)
      }
    })
  }

}
