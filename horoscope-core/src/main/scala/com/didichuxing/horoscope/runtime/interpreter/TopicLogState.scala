package com.didichuxing.horoscope.runtime.interpreter

import com.didichuxing.horoscope.core.Flow.Topic
import com.didichuxing.horoscope.core.FlowRuntimeMessage._
import com.didichuxing.horoscope.runtime.interpreter.TopicLogState.LogTriggerKey
import com.didichuxing.horoscope.runtime.{NULL, Value}
import com.didichuxing.horoscope.util.{Logging, Utils}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.Try

/** *
 * 就近埋点原则：当中心Flow被执行多次时记录多条日志。
 * 当埋点Flow为非中心Flow，被执行多次时，以离中心Flow最近的执行为准。也即，
 * 如果是前向Flow，则记录最后一次；如果是后向Flow，则记录第一次。
 */

trait TopicLogState {
  def deduce(): Seq[TopicLogState]

  def log(): Unit
}

class InitLogState(
  topic: Topic, event: FlowEvent, interpreter: FlowInterpreter
) extends TopicLogState {

  private val original = interpreter.OriginalLog
  private val forward = interpreter.ForwardLog
  private val backward = interpreter.BackwardLog
  private val topicName = topic.name

  override def deduce(): Seq[TopicLogState] = {
    val triggers = getLogTriggers()
    if (triggers.nonEmpty) {
      triggers.flatMap(new BackwardLogState(topic, event, interpreter, _).deduce())
    } else if (forward.flows.contains(topicName)) {
      new ForwardLogState(topic, event, interpreter).deduce()
    } else {
      val topicFlows = topic.fields.map(_.flow)
      val shouldCollect = topicFlows.exists(original.flows.contains)
      if (shouldCollect) {
        new ForwardLogState(topic, event, interpreter).deduce()
      } else {
        Seq(this)
      }
    }
  }

  private def getLogTriggers(): Seq[LogTriggerKey] = {
    val triggerList = event.getTriggerList.asScala ++ original.tokenContext.values.flatMap(_.getTriggerList)
    triggerList.distinct.filter(_.getTopicName == topicName).map({ t =>
      LogTriggerKey(t.getLogId, t.getTopicName, t.getEventId, t.getScopeList.toList)
    }).filter(backward.flows.contains)
  }

  override def log(): Unit = {}
}

// TODO isForward, 暂时由用户输入, 后续由框架推断
class ForwardLogState(
  topic: Topic, event: FlowEvent, interpreter: FlowInterpreter
) extends TopicLogState {
  private val topicName = topic.name
  private val original = interpreter.OriginalLog
  private val forward = interpreter.ForwardLog
  private val backward = interpreter.BackwardLog
  private val instance = interpreter.Main.instance
  private val graph = interpreter.flowGraph

  override def deduce(): Seq[TopicLogState] = {
    collectLogFields()
    if (switchState) {
      // Many times the key flow are executed,  multiple records are logged
      original.flows(topic.flow).toSeq.flatMap { scopesString =>
        val logId = getLogId()
        val logTrigger = LogTriggerKey(logId, topicName, event.getEventId, scopesString.split(","))
        toBackward(logTrigger)
        val scopeList = scopesString.split(",")
        new BackwardLogState(topic, event, interpreter, logTrigger, scopeList).deduce()
      }
    } else {
      Seq(this)
    }
  }

  private def getLogId(): String = "&" + Utils.getEventId()

  override def log(): Unit = {
    if (forward.flows(topicName).nonEmpty) {
      lazy val context = forward.toForward(topicName)
      // set forward context for related schedule event
      for (event <- original.scheduleEvents) {
        val shouldWaiting = graph.hasPath(event.getFlowName, topic.flow)
        if (shouldWaiting) event.addForward(context)
      }

      // set forward context for related callback context
      for (tb <- instance.getTokenBuilderList.asScala) {
        val shouldWaiting = tb.getAsyncFlowList().toList.exists({ flow =>
          graph.hasPath(flow, topic.flow)
        })
        if (shouldWaiting) tb.addForward(context)
      }
    }
  }

  private def switchState: Boolean = original.flows.contains(topic.flow)

  private def collectLogFields(): Unit = {
    // 前向Flow取离中心Flow最远的，也即scope最长的
    val op: String = "_max"
    for (each <- topic.tagFields if each.isForward) {
      if (original.flows.contains(each.flow)) {
        val variable = TraceVariable.newBuilder()
          .setReference(ValueReference.newBuilder().setName(each.name).setFlowName(each.flow))
          .setValue(Value(true).as[FlowValue])
        if (topic.detailed) {
          variable.setTimestamp(System.currentTimeMillis())
          variable.getReferenceBuilder.setEventId(event.getEventId)
        }
        forward.addFlow(topicName, each.name, variable.build())
      }
    }
    for (each <- topic.choiceFields if each.isForward) {
      if (original.choices.contains(each.flow)) {
        val variable = TraceVariable.newBuilder()
          .setReference(ValueReference.newBuilder().setName(each.name))
          .setValue(Value(original.getChoice(each.flow, op)).as[FlowValue])
        if (topic.detailed) {
          variable.setTimestamp(System.currentTimeMillis())
          variable.getReferenceBuilder.setEventId(event.getEventId).setFlowName(each.flow)
        }
        forward.addChoice(topicName, each.name, variable.build())
      }
    }
    for (each <- topic.variableFields if each.isForward) {
      if (original.flows.contains(each.flow)) {
        val exprContext = original.getVariables(each.flow, op).map(r => r._1.name -> r._2)
        val value = Try(each.expression.get.apply(Value(exprContext))).getOrElse(NULL)
        val variable = TraceVariable.newBuilder()
          .setReference(ValueReference.newBuilder().setName(each.name))
          .setValue(Value(value).as[FlowValue])
        if (topic.detailed) {
          variable.setTimestamp(System.currentTimeMillis())
          variable.getReferenceBuilder.setEventId(event.getEventId).setFlowName(each.flow)
        }
        forward.addVariable(topicName, each.name, variable.build())
      }
    }
  }

  private def toBackward(logTrigger: LogTriggerKey): Unit = {
    backward.setDependencyFlows(logTrigger, topic.fields.map(_.flow).toSet)
    forward.flows(topicName).foreach { e =>
      backward.addFlow(logTrigger, e._1, e._2.getReference.getFlowName, e._2)
    }
    forward.choices(topicName).foreach { e =>
      backward.addChoice(logTrigger, e._1, e._2)
    }
    forward.variables(topicName).foreach { e =>
      backward.addVariable(logTrigger, e._1, e._2)
    }
  }
}

/**
 * Note about scopes:
 *    when backwards recovery from trace context, scopes are lost;
 *    when backwards come from forward, scopes are kept.
 */
class BackwardLogState(
  topic: Topic, event: FlowEvent, interpreter: FlowInterpreter,
  trigger: LogTriggerKey, scopes: Seq[String] = Nil) extends TopicLogState with Logging {
  private val original = interpreter.OriginalLog
  private val backward = interpreter.BackwardLog
  private val instance = interpreter.Main.instance
  private val topicName = topic.name
  private val graph = interpreter.flowGraph
  private val keyFlowScopes = trigger.scopes

  override def deduce(): Seq[TopicLogState] = {
    collectLogFields()
    val isTerminated = switchState()
    if (isTerminated) {
      Seq(new TerminateLogState(topic, event, interpreter, trigger))
    } else {
      Seq(this)
    }
  }

  /**
   * 1. Generate backward context
   * 2. Put backward context log trigger to [related] schedule or callback event。When [related] schedule or
   * callback event is triggered, the backward context will be restored and be completed。
   *
   * How to decide [related] schedule or callback, especially when there are multi same-named flow events ?
   * 1): The backward waiting async flows contain the callback or the schedule event flow
   * 2): The keyFlowScopes is prefix of schedule event or callback event scopes to distinguish the case:
   * S -> A -> E
   * S -> B -> E
   * 3): When condition 1) and 2) are satisfied, choose the longest scopes matched trigger to distinguish the case:
   * S -> A -> E
   * S -> A -> B -> E
   */
  override def log(): Unit = {
    val backwardContext = backward.toBackward(trigger)
    instance.addBackward(backwardContext)

    val keyFlowScopes = trigger.scopes
    val asyncFlows = backward.asyncFlows(trigger)
    val triggerBuilder = LogTrigger.newBuilder()
      .setTopicName(topicName).setLogId(trigger.logId)
      .setEventId(trigger.eventId)
      .addAllScope(trigger.scopes.asJava)

    for (schedule <- original.scheduleEvents) {
      val isTraceKept = schedule.getTraceId == schedule.getParent.getTraceId
      val isAsyncWaitingFlow = asyncFlows.contains(schedule.getFlowName)
      val scheduleScopes = schedule.getParent.getScopeList.asScala
      val isScopeMatched = scheduleScopes.startsWith(keyFlowScopes)
      if (isTraceKept && isScopeMatched && isAsyncWaitingFlow) {
        val scheduleTriggerList = schedule.getTriggerList.asScala
        val maxTriggerScopeLength = Try(scheduleTriggerList.map(_.getScopeCount).max).getOrElse(0)
        val keyFlowScopeLength = trigger.scopes.length
        if (maxTriggerScopeLength == keyFlowScopeLength) {
          schedule.addTrigger(triggerBuilder)
        } else if (maxTriggerScopeLength < keyFlowScopeLength) {
          schedule.clearTrigger()
          schedule.addTrigger(triggerBuilder)
        }
      }
    }

    for (builder <- instance.getTokenBuilderList.asScala) {
      val isAsyncWaiting = asyncFlows.intersect(builder.getAsyncFlowList.toSet).nonEmpty
      val tokenScopes = builder.getScopeList.toList
      val isScopeMatched = tokenScopes.startsWith(keyFlowScopes)
      if (isAsyncWaiting && isScopeMatched) {
        val tokenTriggerList = builder.getTriggerList.asScala
        val maxTriggerScopeLength = Try(tokenTriggerList.map(_.getScopeCount).max).getOrElse(0)
        val keyFlowScopeLength = trigger.scopes.length
        if (maxTriggerScopeLength == keyFlowScopeLength) {
          builder.addTrigger(triggerBuilder)
        } else if (maxTriggerScopeLength < keyFlowScopeLength) {
          builder.clearTrigger()
          builder.addTrigger(triggerBuilder)
        }
      }
    }
  }

  private def clearAsyncDeps(): Unit = {
    if (backward.asyncFlows.contains(trigger)) {
      val asyncFlows: Set[String] = backward.asyncFlows(trigger)
      val hasExecuted = for (each <- asyncFlows) yield {
        val dualFlow = graph.getDualFlow(each).toSet
        if (original.flows.keySet.exists(f => f == each || dualFlow.contains(f))) Some(each) else None
      }
      val afterClear = asyncFlows -- hasExecuted.flatten
      backward.asyncFlows += (trigger -> afterClear)
    }
  }

  /** *
   *
   * scopeMatch to distinguish between the following two cases:
   * S -> A(model) -> E1(callback)
   * S -> B(model) -> E2(callback) ,
   * but helpless between the following two cases:
   * S -> A(model) -> E1(callback)
   * S -> A(model) -> B(model) -> E2(callback)
   */
  private def collectAsyncDeps(): Unit = {
    var asyncFlows: Set[String] = backward.asyncFlows(trigger)
    try {
      if (backward.flows.contains(trigger)) {
        if (instance.getScheduleList.nonEmpty || original.callbackFlows.nonEmpty) {
          val remaining = backward.dependencyFlows(trigger).filterNot(flow => {
            backward.asyncFlows(trigger).exists(start =>
              graph.hasPath(start, flow)
            )
          })
          for (flow <- remaining) {
            // schedule
            val scheduleTarget = instance.getScheduleList.find({ schedule =>
              val isTraceKept = schedule.getParent.getTraceId == schedule.getTraceId
              val isScopeMatched = schedule.getParent.getScopeList.asScala.startsWith(keyFlowScopes)
              val isPathReachable = graph.hasPath(schedule.getFlowName, flow)
              isTraceKept && isScopeMatched && isPathReachable
            }).map(_.getFlowName)

            // callback
            val callbackTarget = original.callbackFlows.find({ callback =>
              val isScopeMatched = callback._2.startsWith(keyFlowScopes)
              val isPathReachable = graph.hasPath(callback._1, flow)
              isScopeMatched && isPathReachable
            }).map(_._1)

            // for callback flow, add dual flow
            val totalAsync = (scheduleTarget ++ callbackTarget).flatMap(e => Seq(e) ++ graph.getDualFlow(e).toSeq)
            if (totalAsync.nonEmpty) asyncFlows ++= totalAsync
          }
          backward.asyncFlows += (trigger -> asyncFlows)
        }
      }
    } catch {
      case e: Throwable =>
        error(s"got an exception when detecting terminate state for topic: ${topicName}, msg: ${e.getMessage}")
    }
  }

  private def switchState(): Boolean = {
    collectAsyncDeps()
    clearAsyncDeps()
    val asyncFlow = backward.asyncFlows(trigger)
    val remaining = backward.dependencyFlows(trigger)
    remaining.isEmpty || asyncFlow.isEmpty
  }

  private def collectLogFields(): Unit = {
    val scopesString = scopes.mkString(",")

    def containsFlow(flow: String): Boolean = original.flows(flow).exists(_.startsWith(scopesString))

    // 后向Flow取离中心Flow最近的，也即scope最短的
    val op: String = "_min"
    backward.setDependencyFlows(trigger, topic.fields.map(_.flow).toSet)
    topic.variableFields.filter(!_.isForward).foreach { e =>
      if (containsFlow(e.flow)) {
        val exprContext = original.getVariables(e.flow, op, scopesString).map(r => r._1.name -> r._2)
        val value = Try(e.expression.get.apply(Value(exprContext))).getOrElse(NULL)
        val variable = TraceVariable.newBuilder().setValue(value.as[FlowValue])
          .setReference(ValueReference.newBuilder().setName(e.name))
        if (topic.detailed) {
          variable.getReferenceBuilder.setFlowName(e.flow).setEventId(event.getEventId)
          variable.setTimestamp(System.currentTimeMillis()).build()
        }
        backward.addVariable(trigger, e.name, variable.build())
      }
    }

    topic.choiceFields.filter(!_.isForward).foreach { e =>
      if (containsFlow(e.flow)) {
        val variable = TraceVariable.newBuilder()
          .setValue(Value(original.getChoice(e.flow, op, scopesString)).as[FlowValue])
          .setReference(ValueReference.newBuilder().setName(e.name))
        if (topic.detailed) {
          variable.getReferenceBuilder.setFlowName(e.flow).setEventId(event.getEventId)
          variable.setTimestamp(System.currentTimeMillis())
        }
        backward.addChoice(trigger, e.name, variable.build())
      }
    }

    topic.tagFields.filter(!_.isForward).foreach { e =>
      if (containsFlow(e.flow)) {
        val variable = TraceVariable.newBuilder()
          .setValue(Value(true).as[FlowValue])
          .setReference(ValueReference.newBuilder().setName(e.name))
        if (topic.detailed) {
          variable.getReferenceBuilder.setFlowName(e.flow).setEventId(event.getEventId)
          variable.setTimestamp(System.currentTimeMillis())
        }
        backward.addFlow(trigger, e.name, e.flow, variable.build())
      }
    }
  }
}

class TerminateLogState(
  topic: Topic, event: FlowEvent, interpreter: FlowInterpreter, trigger: LogTriggerKey
) extends TopicLogState {
  private val instance = interpreter.Main.instance
  private val backward = interpreter.BackwardLog
  private val original = interpreter.OriginalLog

  override def log(): Unit = {
    instance.addTopic(backward.toTopic(trigger, topic))
    original.expiredLogIds ::= trigger.logId
  }

  override def deduce(): Seq[TopicLogState] = {
    Seq(this)
  }
}

object TopicLogState {

  case class LogTriggerKey(logId: String, topicName: String, eventId: String, scopes: Seq[String] = Nil)

  def newBuilder: TopicLogStateBuilder = {
    new TopicLogStateBuilder()
  }

  class TopicLogStateBuilder {
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
