/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.service.source

import com.didichuxing.horoscope.core.{EventBus, SyncEventBus}
import com.didichuxing.horoscope.core.FlowRuntimeMessage.{FlowEvent, FlowInstance}
import com.didichuxing.horoscope.runtime.Value
import com.didichuxing.horoscope.service.ApplicationContext
import com.didichuxing.horoscope.util.Logging
import com.typesafe.config.Config

abstract class AbstractEventBus(sourceName: String, flowName: String, params: Config)
                               (implicit ctx: ApplicationContext) extends SyncEventBus with Logging {

  private val multiScheduler = ctx.multiScheduler
  protected val eventProcessor: EventProcessor = newEventProcessor()

  override def start(): Unit = {
    eventProcessor.start()
    multiScheduler.start(sourceName, params, this)
    debug("event bus start")
  }

  override def stop(): Unit = {
    multiScheduler.stop(sourceName)
    eventProcessor.stop()
  }

  def newEventProcessor(): EventProcessor

  override def process(sourceEvents: List[Value]): List[FlowEvent] = {
    //返回带序号的flowEvents
    val seqFlowEvents = EventBuilders.flowEventBuilder(sourceEvents, sourceName, flowName, params)(ctx.builtIn)
    //排除格式错误的消息列表
    val processEvents = seqFlowEvents.filter(v => v.flowEvent != null)
    //按顺序返回处理后的消息列表
    if (processEvents.size > 0) {
      val resultEvents = doProcess(processEvents.map(v => v.flowEvent))
      //合并成功的，失败的和格式错误的消息
      mergeEvents(seqFlowEvents, processEvents, resultEvents)
    } else {
      mergeEvents(seqFlowEvents, processEvents, List[FlowEvent]())
    }
  }

  def mergeEvents(seqFlowEvents: List[SeqFlowEvent], processEvents: List[SeqFlowEvent],
                  resultEvents: List[FlowEvent]): List[FlowEvent] = {
    if (processEvents.size == resultEvents.size) {
      //成功处理的消息列表
      val successEvents = new Array[FlowEvent](seqFlowEvents.size)
      for ((event, i) <- resultEvents.zipWithIndex) {
        val processEvent = processEvents(i)
        if (event != null) {
          successEvents(processEvent.seq) = event
        } else {
          //失败的消息，可能失败的原因包括反压超时，追加mailbox失败
          warn("event process error")
        }
      }
      //格式错误的也认为是成功的消息，不处理
      val skipEvents = seqFlowEvents.filter(v => v.flowEvent == null)
      for (seqEvent <- skipEvents) {
        //返回一个空的flowEvent，表示已经忽略处理完成
        successEvents(seqEvent.seq) = FlowEvent.getDefaultInstance
      }
      //返回全部消息，失败的不赋值
      successEvents.toList
    } else {
      //如果返回的数量不一致，全部返回失败
      error("process event return size error")
      new Array[FlowEvent](seqFlowEvents.size).toList
    }
  }

  override def processSync(value: Value): FlowInstance = {
    //返回带序号的flowEvents
    val seqFlowEvents = EventBuilders.flowEventBuilder(List[Value](value), sourceName, flowName, params)(ctx.builtIn)
    //排除格式错误的消息列表
    val processEvents = seqFlowEvents.filter(v => v.flowEvent != null)
    //按顺序返回处理后的消息列表
    if (processEvents.size > 0) {
      doProcessSync(processEvents(0).flowEvent)
    } else {
      //格式错误
      throw EventProcessException(EventProcessErrorCode.FormatError)
    }
  }

  def doProcessSync(event: FlowEvent): FlowInstance

}
