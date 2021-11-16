package com.didichuxing.horoscope.runtime.interpreter

import com.didichuxing.horoscope.core.Flow
import com.didichuxing.horoscope.core.Flow._
import com.didichuxing.horoscope.core.FlowRuntimeMessage._
import com.didichuxing.horoscope.runtime.interpreter.TopicLogCollector.LogIdentity
import com.didichuxing.horoscope.runtime.{NULL, Value}
import com.didichuxing.horoscope.util.{Logging, Utils}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
 * 就近埋点原则：当中心Flow被执行多次时记录多条日志。
 *             当埋点Flow为非中心Flow，被执行多次时，以离中心Flow最近的执行为准。也即，
 *               如果是前向Flow，则记录最后一次；如果是后向Flow，则记录第一次。
 */

trait TopicLogState {
  def topic: Topic

  def event: FlowEvent

  def deduce(): Seq[TopicLogState]

  def log(): Unit
}

class InitialLogState(
  val topic: Topic, val event: FlowEvent, interpreter: FlowInterpreter
) extends TopicLogState {

  private val original = interpreter.Log.original
  private val forward = interpreter.Log.forward
  private val backward = interpreter.Log.backward
  private val topicName = topic.name

  override def deduce(): Seq[TopicLogState] = {
    val triggers = getBackwardLogTriggers()
    if (triggers.nonEmpty) {
      triggers.flatMap(trigger => new BackwardLogState(topic, event, interpreter, trigger).deduce())
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

  private def getBackwardLogTriggers(): Seq[LogIdentity] = {
    val triggerList = event.getTriggerList.asScala ++ original.tokenContext.values.flatMap(_.getTriggerList)
    triggerList.filter(_.getTopicName == topicName).map({ t =>
      LogIdentity(t.getLogId, t.getTopicName, t.getEventId, t.getScopeList.toList)
    }).filter(backward.flows.contains).distinct
  }

  override def log(): Unit = {}
}

class ForwardLogState(
  val topic: Topic, val event: FlowEvent, interpreter: FlowInterpreter
) extends TopicLogState {
  private val topicName = topic.name
  private val original = interpreter.Log.original
  private val forward = interpreter.Log.forward
  private val backward = interpreter.Log.backward
  private val instance = interpreter.Main.instance
  private val graph = interpreter.flowGraph

  override def deduce(): Seq[TopicLogState] = {
    collectLogFields()
    if (shouldSwitchState) {
      original.flows(topic.keyFlow).toSeq.flatMap { scopesString =>
        val logId = getLogId()
        val scopeList = scopesString.split(",")
        val logTrigger = LogIdentity(logId, topicName, event.getEventId, scopeList)
        toBackward(logTrigger)
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
      for (event <- instance.getScheduleBuilderList) {
        val isPathReachable = graph.hasPath(event.getFlowName, topic.keyFlow)
        if (isPathReachable) event.addForward(context)
      }

      // set forward context for related callback context
      for (tokenContext <- instance.getTokenBuilderList) {
        val isPathReachable = tokenContext.getAsyncFlowList.toList.exists({ flow =>
          graph.hasPath(flow, topic.keyFlow)
        })
        if (isPathReachable) tokenContext.addForward(context)
      }
    }
  }

  private def shouldSwitchState: Boolean = original.flows.contains(topic.keyFlow)

  private def collectLogFields(): Unit = {
    val eventId = event.getEventId
    val timestamp = System.currentTimeMillis()
    // 前向Flow取离中心Flow最远的, 也即scope最长的
    val op: String = "_max"
    for (field <- topic.fields if (field.isForward && original.flows.contains(field.flow))) {
      val reference = ValueReference.newBuilder().setName(field.name).setEventId(eventId).setFlowName(field.flow)
      field.`type` match {
        case VARIABLE_TYPE =>
          val exprContext = original.getVariables(field.flow, op).map(r => r._1.name -> r._2)
          val value = Try(field.expression.get.apply(Value(exprContext))).getOrElse(NULL)
          val variable = TraceVariable.newBuilder().setReference(reference)
            .setValue(value.as[FlowValue])
            .setTimestamp(timestamp)
            .build()
          forward.addVariable(topicName, field.name, variable)
        case CHOICE_TYPE =>
          val value = Value(original.getChoice(field.flow, op))
          val variable = TraceVariable.newBuilder().setReference(reference)
            .setValue(value.as[FlowValue])
            .setTimestamp(timestamp)
            .build()
          forward.addChoice(topicName, field.name, variable)
        case TAG_TYPE =>
          val value = Value(true)
          val variable = TraceVariable.newBuilder().setReference(reference)
            .setValue(value.as[FlowValue])
            .setTimestamp(timestamp)
            .build()
          forward.addFlow(topicName, field.name, variable)
        case _ => Unit
      }
    }
  }

  private def toBackward(logIdentity: LogIdentity): Unit = {
    backward.setDependencyFlows(logIdentity, topic.fields.map(_.flow).toSet)
    forward.flows(topicName).foreach { e =>
      backward.addFlow(logIdentity, e._1, e._2.getReference.getFlowName, e._2)
    }
    forward.choices(topicName).foreach { e =>
      backward.addChoice(logIdentity, e._1, e._2)
    }
    forward.variables(topicName).foreach { e =>
      backward.addVariable(logIdentity, e._1, e._2)
    }
  }
}


/**
 * Note about scopes:
 * when backwards recovery from trace context, scopes are lost;
 * when backwards come from forward, scopes are kept.
 */

class BackwardLogState(
  val topic: Topic, val event: FlowEvent, interpreter: FlowInterpreter,
  identity: LogIdentity, scopes: Seq[String] = Nil) extends TopicLogState with Logging {
  private val original = interpreter.Log.original
  private val backward = interpreter.Log.backward
  private val instance = interpreter.Main.instance
  private val topicName = topic.name
  private val graph = interpreter.flowGraph
  private val keyFlowScopes = identity.scopes

  override def deduce(): Seq[TopicLogState] = {
    collectLogFields()
    val isTerminate = switchState()
    if (isTerminate) Seq(new TerminateLogState(topic, event, interpreter, identity)) else Seq(this)
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
    val backwardContext = backward.toBackward(identity)
    instance.addBackward(backwardContext)

    val keyFlowScopes = identity.scopes
    val asyncFlows = backward.asyncFlows(identity)

    val triggerBuilder = LogTrigger.newBuilder()
      .setTopicName(topicName).setLogId(identity.logId)
      .setEventId(identity.eventId)
      .addAllScope(identity.scopes.asJava)

    for (schedule <- instance.getScheduleBuilderList) {
      val isTraceKept = schedule.getTraceId == schedule.getParent.getTraceId
      val isAsyncWaitingFlow = asyncFlows.contains(schedule.getFlowName)
      val scheduleScopes = schedule.getParent.getScopeList.asScala
      val isScopeMatched = scheduleScopes.startsWith(keyFlowScopes)
      if (isTraceKept && isScopeMatched && isAsyncWaitingFlow) {
        val scheduleTriggerList = schedule.getTriggerList.asScala
        val maxTriggerScopeLength = Try(scheduleTriggerList.map(_.getScopeCount).max).getOrElse(0)
        val keyFlowScopeLength = identity.scopes.length
        if (maxTriggerScopeLength == keyFlowScopeLength) {
          schedule.addTrigger(triggerBuilder)
        } else if (maxTriggerScopeLength < keyFlowScopeLength) {
          schedule.clearTrigger()
          schedule.addTrigger(triggerBuilder)
        }
      }
    }
    for (builder <- instance.getTokenBuilderList) {
      val isAsyncWaiting = asyncFlows.intersect(builder.getAsyncFlowList.toSet).nonEmpty
      val tokenScopes = builder.getScopeList.toList
      val isScopeMatched = tokenScopes.startsWith(keyFlowScopes)
      if (isAsyncWaiting && isScopeMatched) {
        val tokenTriggerList = builder.getTriggerList.asScala
        val maxTriggerScopeLength = Try(tokenTriggerList.map(_.getScopeCount).max).getOrElse(0)
        val keyFlowScopeLength = identity.scopes.length
        if (maxTriggerScopeLength == keyFlowScopeLength) {
          builder.addTrigger(triggerBuilder)
        } else if (maxTriggerScopeLength < keyFlowScopeLength) {
          builder.clearTrigger()
          builder.addTrigger(triggerBuilder)
        }
      }
    }
  }

  // 清理的时候对于callback相关的flow, 只要是对偶flow也要进行清理
  private def clearAsyncDeps(): Unit = {
    if (backward.asyncFlows.contains(identity)) {
      val asyncFlows = backward.asyncFlows(identity)
      val hasExecuted = for (each <- asyncFlows) yield {
        val dualFlow = graph.getDualFlow(each).toSet
        if (original.flows.keySet.exists(f => f == each || dualFlow.contains(f))) Some(each) else None
      }
      val afterClear = asyncFlows -- hasExecuted.flatten
      backward.asyncFlows += (identity -> afterClear)
    }
  }

  private def collectAsyncDeps(): Unit = {
    var asyncFlows: Set[String] = backward.asyncFlows(identity)
    try {
      if (backward.flows.contains(identity)) {
        if (instance.getScheduleList.nonEmpty || original.callbackFlows.nonEmpty) {
          val remaining = backward.dependencyFlows(identity).filterNot(flow => {
            backward.asyncFlows(identity).exists(start =>
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
          backward.asyncFlows += (identity -> asyncFlows)

        }
      }
    } catch {
      case e: Throwable =>
        error(s"got an exception when detecting terminate state for topic: ${topicName}, msg: ${e.getMessage}")
    }
  }

  private def switchState() = {
    collectAsyncDeps()
    clearAsyncDeps()
    val asyncFlow = backward.asyncFlows(identity)
    val remaining = backward.dependencyFlows(identity)
    remaining.isEmpty || asyncFlow.isEmpty
  }

  private def collectLogFields(): Unit = {
    val scopesString = scopes.mkString(",")
    def containsFlow(flow: String) = original.flows(flow).exists(_.startsWith(scopesString))

    // 后向Flow取离中心Flow最近的，也即scope最短的
    val op = "_min"
    backward.setDependencyFlows(identity, topic.fields.map(_.flow).toSet)
    val timestamp = System.currentTimeMillis()
    val eventId = event.getEventId
    for (e <- topic.fields.filter(!_.isForward) if containsFlow(e.flow)) {
      val reference = ValueReference.newBuilder.setName(e.name).setFlowName(e.flow).setEventId(eventId)
      e.`type` match {
        case VARIABLE_TYPE =>
          val exprContext = original.getVariables(e.flow, op, scopes).map(r => r._1.name -> r._2)
          val value = Try(e.expression.get.apply(Value(exprContext))).getOrElse(NULL)
          val variable = TraceVariable.newBuilder().setReference(reference)
            .setValue(value.as[FlowValue])
            .setTimestamp(timestamp)
            .build()
          backward.addVariable(identity, e.name, variable)
        case CHOICE_TYPE =>
          val value = Value(original.getChoice(e.flow, op, scopes))
          val variable = TraceVariable.newBuilder().setReference(reference)
            .setValue(value.as[FlowValue])
            .setTimestamp(timestamp)
            .build()
          backward.addChoice(identity, e.name, variable)
        case TAG_TYPE =>
          val value = Value(true)
          val variable = TraceVariable.newBuilder().setReference(reference)
          .setValue(value.as[FlowValue])
          .setTimestamp(timestamp)
          .build()
          backward.addFlow(identity, e.name, e.flow, variable)
        case _ => Unit
      }
    }
  }
}

class TerminateLogState(
  val topic: Topic, val event: FlowEvent, interpreter: FlowInterpreter, trigger: LogIdentity
) extends TopicLogState {
  private val instance = interpreter.Main.instance
  private val backward = interpreter.Log.backward
  private val original = interpreter.Log.original

  override def log(): Unit = {
    instance.addTopic(backward.toTopic(event.getTraceId, trigger, topic))
    original.loadedLogIds ::= trigger.logId
  }

  override def deduce(): Seq[TopicLogState] = {
    Seq(this)
  }
}

object TopicLogCollector {

  case class VariableKey(name: String, flow: String)

  case class LogIdentity(logId: String, topicName: String, eventId: String, scopes: Seq[String] = Nil)

  def newBuilder: TopicLogStateBuilder = new TopicLogStateBuilder()

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
      new InitialLogState(_topic, _event, _interpreter)
    }
  }

  class OriginalLog {
    // variableKey -> [(scopes, value)]
    val variables: mutable.Map[VariableKey, List[(Seq[String], Value)]] = mutable.Map().withDefaultValue(Nil)
    // flow -> [(scopes, choice)]
    val choices: mutable.Map[String, List[(Seq[String], Seq[String])]] = mutable.Map().withDefaultValue(Nil)
    // flow -> [scopes]
    val flows: mutable.Map[String, Set[String]] = mutable.Map().withDefaultValue(Set())
    // help to determine log state
    val callbackFlows: ListBuffer[(String, Seq[String])] = ListBuffer.empty
    // #tokenId, to help to delete trace context keys
    var loadedTokenIds: List[String] = Nil
    // &logId, to help to delete trace context keys
    var loadedLogIds: List[String] = Nil
    // #token -> tokenContext
    val tokenContext: mutable.Map[String, TokenContext] = mutable.Map()

    def getVariables(flow: String, op: String, scope: Seq[String] = Nil): Map[VariableKey, Value] = {
      variables.filter(_._1.flow == flow).mapValues(v => {
        val scopeMatched = v.filter(_._1.startsWith(scope))
        if (scopeMatched.isEmpty) {
          NULL
        } else {
          val result = op match {
            case "_min" => scopeMatched.minBy(_._1.length)
            case "_max" => scopeMatched.maxBy(_._1.length)
          }
          result._2
        }
      }).toMap
    }

    def getChoice(flow: String, op: String, scopes: Seq[String] = Nil): Seq[String] = {
      val scopeMatched = choices(flow).filter(_._1.startsWith(scopes))
      if (scopeMatched.isEmpty) {
        Nil
      } else {
        op match {
          case "_min" => scopeMatched.minBy(_._1.length)._2
          case "_max" => scopeMatched.maxBy(_._1.length)._2
        }
      }
    }

    def addVariable(key: VariableKey, scopes: Seq[String], value: Value): Unit = {
      val oldValue = variables(key)
      variables += (key -> ((scopes, value) :: oldValue))
    }

    def addChoice(flow: String, scopes: Seq[String], choice: Seq[String]): Unit = {
      val oldValue = choices(flow)
      choices += (flow -> ((scopes, choice) :: oldValue))
    }

    def addFlow(flow: String, scopes: Seq[String]): Unit = {
      val oldValue = flows(flow)
      flows += (flow -> (oldValue ++ Set(scopes.mkString(","))))
    }
  }

  class ForwardLog {
    // [topic -> [name -> value]]
    val variables: mutable.Map[String, Map[String, TraceVariable]] = mutable.Map().withDefaultValue(Map())

    // [topic -> [name -> choices]
    val choices: mutable.Map[String, Map[String, TraceVariable]] = mutable.Map().withDefaultValue(Map())

    // [topic -> (name, true)]
    val flows: mutable.Map[String, Map[String, TraceVariable]] = mutable.Map().withDefaultValue(Map())

    def addFlow(topic: String, name: String, variable: TraceVariable): Unit = {
      var map = flows(topic)
      map += (name -> variable)
      flows += (topic -> map)
    }

    def addChoice(topic: String, name: String, variable: TraceVariable): Unit = {
      var map = choices(topic)
      map += (name -> variable)
      choices += (topic -> map)
    }

    def addVariable(topic: String, name: String, variable: TraceVariable): Unit = {
      var map = variables(topic)
      map += (name -> variable)
      variables += (topic -> map)
    }

    def addContext(forward: ForwardContext): Unit = {
      val topic = forward.getTopicName
      flows += (topic -> forward.getTagList.asScala.map(e => e.getReference.getName -> e).toMap)
      choices += (topic -> forward.getChoiceList.asScala.map(e => e.getReference.getName -> e).toMap)
      variables += (topic -> forward.getVariableList.asScala.map(e => e.getReference.getName -> e).toMap)
    }

    def toForward(topic: String): ForwardContext = {
      ForwardContext.newBuilder()
        .setTopicName(topic)
        .addAllVariable(variables(topic).values.asJava)
        .addAllTag(flows(topic).values.asJava)
        .addAllChoice(choices(topic).values.asJava)
        .build()
    }
  }

  class BackwardLog {
    // logKey -> name -> value
    val variables: mutable.Map[LogIdentity, Map[String, TraceVariable]] = mutable.Map().withDefaultValue(Map())
    // logKey -> name -> choices
    val choices: mutable.Map[LogIdentity, Map[String, TraceVariable]] = mutable.Map().withDefaultValue(Map())
    // logKey -> [(name, flowName)]
    val flows: mutable.Map[LogIdentity, Map[String, TraceVariable]] = mutable.Map().withDefaultValue(Map())
    // logKey -> [flow]
    val dependencyFlows: mutable.Map[LogIdentity, Set[String]] = mutable.Map().withDefaultValue(Set())

    // 等待异步执行的flow
    val asyncFlows: mutable.Map[LogIdentity, Set[String]] = mutable.Map().withDefaultValue(Set())

    def addVariable(key: LogIdentity, name: String, variable: TraceVariable): Unit = {
      var map = variables(key)
      // 保证就近原则
      if (!map.contains(name)) {
        map += (name -> variable)
        variables += (key -> map)
      }
    }

    def addChoice(key: LogIdentity, name: String, variable: TraceVariable): Unit = {
      var map = choices(key)
      // 保证就近原则
      if (!map.contains(name)) {
        map += (name -> variable)
      }
      choices += (key -> map)
    }

    def addFlow(key: LogIdentity, name: String, flow: String, variable: TraceVariable): Unit = {
      var map = flows(key)
      if (!map.contains(name)) {
        map += (name -> variable)
        flows += (key -> map)
        removeDependencyFlows(key, Set(flow))
      }
    }

    def setDependencyFlows(key: LogIdentity, flows: Set[String]): Unit = {
      if (!dependencyFlows.contains(key)) {
        dependencyFlows += (key -> flows)
      }
    }

    def removeDependencyFlows(key: LogIdentity, flows: Set[String]): Unit = {
      val value = dependencyFlows(key)
      dependencyFlows += (key -> (value -- flows))
    }

    def addContext(key: LogIdentity, context: BackwardContext): Unit = {
      flows += (key -> context.getTagList.asScala.map(e => e.getReference.getName -> e).toMap)
      choices += (key -> context.getChoiceList.asScala.map(e => e.getReference.getName -> e).toMap)
      variables += (key -> context.getVariableList.asScala.map(e => e.getReference.getName -> e).toMap)
      dependencyFlows += (key -> context.getDependencyFlowList.toSet)
      asyncFlows += (key -> context.getAsyncFlowList.toSet)
    }

    def toBackward(key: LogIdentity): BackwardContext = {
      BackwardContext.newBuilder()
        .setTopicName(key.topicName)
        .setLogId(key.logId)
        .addAllChoice(choices(key).values.asJava)
        .addAllTag(flows(key).values.asJava)
        .addAllVariable(variables(key).values.asJava)
        .addAllDependencyFlow(dependencyFlows(key).asJava)
        .addAllAsyncFlow(asyncFlows(key).asJava)
        .addAllScope(key.scopes.asJava)
        .build()
    }

    def toTopic(traceId: String, key: LogIdentity, topic: Flow.Topic): FlowInstance.Topic = {
      val flowTagMap = topic.tagFields.map(r => (r.flow, r.name)).toMap
      val builder = FlowInstance.Topic.newBuilder()
        .setTraceId(traceId)
        .setTopicName(key.topicName)
        .setLogId(key.logId.substring(1)) // to remove '&'
        .setLogTimestamp(System.currentTimeMillis())
        .addAllScope(key.scopes)
      val variableFields = variables(key).mapValues(_.getValue)
      val choiceFields = choices(key).mapValues(_.getValue)
      val tagFields = flows(key).mapValues(_.getValue)
      val unTriggeredTags = dependencyFlows(key).map {r => (flowTagMap(r), Value(false).as[FlowValue])}.toMap
      val fields = (variableFields ++ choiceFields ++ tagFields ++ unTriggeredTags)
      builder.putAllField(fields)

      if (topic.detailed) {
        val triggered = (variables ++ choices ++ flows).getOrElse(key, Map()).mapValues(v =>
          Value(v.getReference.toBuilder.clearName()).as[FlowValue]
        )
        // 未触发的
        val unTriggered = dependencyFlows(key).map(flowName => {
          val tagName = flowTagMap(flowName)
          tagName -> Value(Map("flow_name" -> Value(flowName))).as[FlowValue]
        }).toMap
        val details = triggered ++ unTriggered
        builder.putAllDetail(details)
      }
      builder.build()
    }
  }

}
