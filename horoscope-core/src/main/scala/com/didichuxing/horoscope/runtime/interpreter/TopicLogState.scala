package com.didichuxing.horoscope.runtime.interpreter

import com.didichuxing.horoscope.core.Flow.Topic
import com.didichuxing.horoscope.core.FlowRuntimeMessage._
import com.didichuxing.horoscope.runtime.interpreter.TopicLogState.LogTriggerKey
import com.didichuxing.horoscope.runtime.{NULL, Value}
import com.didichuxing.horoscope.util.{Logging, Utils}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.Try

/***
 * 就近埋点原则：当中心Flow被执行多次时记录多条日志。
 *             当埋点Flow为非中心Flow，被执行多次时，以离中心Flow最近的执行为准。也即，
 *               如果是前向Flow，则记录最后一次；如果是后向Flow，则记录第一次。
 */

trait TopicLogState {
  def deduce(): Seq[TopicLogState]

  def log(): Unit
}

class InitLogState(
  topic: Topic, event: FlowEvent, interpreter: FlowInterpreter
) extends TopicLogState {

  private val original = interpreter.Log
  private val forward = interpreter.ForwardLog
  private val topicName = topic.name

  override def deduce(): Seq[TopicLogState] = {
    val triggers = getLogTriggers
    if (triggers.nonEmpty) {
      triggers.flatMap(trigger =>
        new BackwardLogState(topic, event, interpreter, trigger).deduce()
      )
    } else if (forward.flows.contains(topicName)) {
      new ForwardLogState(topic, event, interpreter).deduce()
    } else {
      val targetFlows = topic.fields.map(_.flow)
      val needCollecting = targetFlows.exists(original.flows.contains)
      if (needCollecting) {
        new ForwardLogState(topic, event, interpreter).deduce()
      } else {
        Seq(this)
      }
    }
  }

  private def getLogTriggers: Seq[LogTriggerKey] = {
    val triggerList = event.getTriggerList.asScala ++ original.tokenContext.values.map(_.getTrigger)
    triggerList.filter(_.getTopicName == topicName).map { t =>
      LogTriggerKey(t.getLogId, t.getTopicName, t.getEventId)
    }
  }

  override def log(): Unit = {}
}

// TODO isForward, 暂时由用户输入, 后续由框架推断
class ForwardLogState(
  topic: Topic, event: FlowEvent, interpreter: FlowInterpreter
) extends TopicLogState {
  private val topicName = topic.name
  private val original = interpreter.Log
  private val forward = interpreter.ForwardLog
  private val backward = interpreter.BackwardLog
  private val instance = interpreter.Main.instance

  override def deduce(): Seq[TopicLogState] = {
    collectFields()
    if (isKeyFlowTriggered) {
      original.flows(topic.flow).toSeq.flatMap { scope =>
        val logId = getLogId()
        val logTrigger = LogTriggerKey(logId, topicName, event.getEventId)
        toBackward(logTrigger)
        new BackwardLogState(topic, event, interpreter, logTrigger, scope).deduce()
      }
    } else {
      Seq(this)
    }
  }

  private def getLogId(): String = "&" + Utils.getEventId()


  // TODO topic无关schedule和callbackEvent进行日志裁剪
  override def log(): Unit = {
    val schedules = original.scheduleEvents
    lazy val context = forward.toForward(topicName)
    // set forward context for schedule event
    for (event <- schedules) event.setForward(context)
    // set forward context for callback
    instance.getTokenBuilderList.asScala.foreach { t =>
      t.setForward(context)
    }
  }

  private def isKeyFlowTriggered: Boolean = {
    original.flows.contains(topic.flow)
  }

  private def collectFields(): Unit = {
    // 前向Flow取离中心Flow最远的，也即scope最长的
    val op: String = "_max"
    for (each <- topic.tagFields if each.isForward) {
      if (original.flows.contains(each.flow)) {
        forward.addFlow(topicName, each.name, each.flow)
      }
    }
    for (each <- topic.choiceFields if each.isForward) {
      if (original.choices.contains(each.flow)) {
        forward.addChoice(topicName, each.name, original.getChoice(each.flow, op))
      }
    }
    for (each <- topic.variableFields if each.isForward) {
      if (original.flows.contains(each.flow)) {
        val exprContext = original.getVariables(each.flow, op).map(r => r._1.name -> r._2)
        val value = Try(each.expression.get.apply(Value(exprContext))).getOrElse(NULL)
        forward.addVariable(topicName, each.name, value)
      }
    }
  }

  private def toBackward(logTrigger: LogTriggerKey): Unit = {
    backward.setDependencyFlows(logTrigger, topic.fields.map(_.flow).toSet)
    forward.flows.getOrElse(topicName, Nil).foreach { t =>
      backward.addFlow(logTrigger, t._1, t._2)
    }
    forward.choices.getOrElse(topicName, Map()).foreach { c =>
      backward.addChoice(logTrigger, c._1, c._2)
    }
    forward.variables.getOrElse(topicName, Map()).foreach { v =>
      backward.addVariable(logTrigger, v._1, v._2)
    }
  }
}

class BackwardLogState(
  topic: Topic, event: FlowEvent, interpreter: FlowInterpreter,
  trigger: LogTriggerKey, scopes: String = "") extends TopicLogState with Logging {
  private val original = interpreter.Log
  private val backward = interpreter.BackwardLog
  private val instance = interpreter.Main.instance
  private val topicName = topic.name

  override def deduce(): Seq[TopicLogState] = {
    collectFields()
    if (isTerminated) {
      Seq(new TerminateLogState(topic, event, interpreter, trigger))
    } else {
      Seq(this)
    }
  }

  override def log(): Unit = {
    val backwardContext = backward.toBackward(trigger)
    instance.addBackward(backwardContext)
    val triggerBuilder = LogTrigger.newBuilder()
      .setTopicName(topicName).setLogId(trigger.logId)
      .setEventId(trigger.eventId)
    for (schedule <- original.scheduleEvents) {
      if (schedule.getTraceId == schedule.getParent.getTraceId) {
        schedule.addTrigger(triggerBuilder)
      }
    }
    instance.getTokenBuilderList.asScala.foreach { builder =>
      builder.setTrigger(triggerBuilder)
    }
  }

  private def isTerminated: Boolean = {
    var terminated: Boolean = true
    try {
      if (backward.flows.contains(trigger)) {
        if (instance.getScheduleList.nonEmpty || original.callbackFlows.nonEmpty) {
          val remaining = backward.dependencyFlows.getOrElse(trigger, Set())
          for (end <- remaining if terminated) {
            val hasSchedule = instance.getScheduleList.asScala.exists({ start =>
              val isTraceKept = start.getParent.getTraceId == start.getTraceId
              isTraceKept && interpreter.flowGraph.hasPath(start.getFlowName, end)
            })
            val hasCallback = original.callbackFlows.exists({ start =>
              interpreter.flowGraph.hasPath(start, end)
            })
            terminated = !hasSchedule && !hasCallback
          }
        }
      }
    } catch {
      case e: Throwable =>
        error(s"got an exception when detecting terminate state for topic: ${topicName}, msg: ${e.getMessage}")
    }

    terminated
  }

  private def collectFields(): Unit = {
    def containsFlow(flow: String): Boolean = {
      original.flows.contains(flow) && original.flows(flow).exists(_.startsWith(scopes))
    }
    // 后向Flow取离中心Flow最近的，也即scope最短的
    val op: String = "_min"
    backward.setDependencyFlows(trigger, topic.fields.map(_.flow).toSet)
    topic.variableFields.filter(!_.isForward).foreach { each =>
      if (containsFlow(each.flow)) {
        val exprContext = original.getVariables(each.flow, op, scopes).map(r => r._1.name -> r._2)
        val value = Try(each.expression.get.apply(Value(exprContext))).getOrElse(NULL)
        backward.addVariable(trigger, each.name, value)
      }
    }

    topic.choiceFields.filter(!_.isForward).foreach { c =>
      if (containsFlow(c.flow)) {
        backward.addChoice(trigger, c.name, original.getChoice(c.flow, op, scopes))
      }
    }

    topic.tagFields.filter(!_.isForward).foreach { t =>
      if (containsFlow(t.flow)) {
        backward.addFlow(trigger, t.name, t.flow)
      }
    }
  }
}

class TerminateLogState(
  topic: Topic, event: FlowEvent, interpreter: FlowInterpreter, trigger: LogTriggerKey
) extends TopicLogState {
  private val instance = interpreter.Main.instance
  private val backward = interpreter.BackwardLog
  private val original = interpreter.Log

  override def log(): Unit = {
    instance.addTopic(backward.toTopic(trigger, topic))
    original.expiredLogIds ::= trigger.logId
  }

  override def deduce(): Seq[TopicLogState] = {
    Seq(this)
  }
}

object TopicLogState {

  case class LogTriggerKey(logId: String, topicName: String, eventId: String)

  def newBuilder: BuriedPointLogBuilder = {
    new BuriedPointLogBuilder()
  }

  class BuriedPointLogBuilder {
    private var _event: FlowEvent = null
    private var _topic: Topic = null
    private var _interpreter: FlowInterpreter = null

    def withEvent(event: FlowEvent): this.type = {
      _event = event
      this
    }

    def withTopic(topic: Topic): this.type = {
      _topic = topic
      this
    }

    def withInterpreter(interpreter: FlowInterpreter): this.type = {
      _interpreter = interpreter
      this
    }

    def build(): TopicLogState = {
      new InitLogState(_topic, _event, _interpreter)
    }
  }
}
