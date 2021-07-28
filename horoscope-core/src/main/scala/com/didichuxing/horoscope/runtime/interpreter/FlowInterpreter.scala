/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.runtime.interpreter

import java.io.{PrintWriter, StringWriter}
import java.util.concurrent.ExecutionException

import akka.actor.{Actor, ActorRef}
import com.didichuxing.horoscope.core.Flow
import com.didichuxing.horoscope.core.Flow._
import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowInstance.{Experiment, Procedure, Topic}
import com.didichuxing.horoscope.core.FlowRuntimeMessage._
import com.didichuxing.horoscope.dsl.{SemanticException, SyntaxException}
import com.didichuxing.horoscope.runtime.FlowExecutorImpl.CommitEvent
import com.didichuxing.horoscope.runtime.{Value, _}
import com.didichuxing.horoscope.runtime.experiment.ExperimentController.{ExperimentPlan, FlowOption}
import com.didichuxing.horoscope.runtime.expression.Expression
import com.didichuxing.horoscope.runtime.interpreter.TopicLogState.LogTriggerKey
import com.didichuxing.horoscope.util.Logging
import com.didichuxing.horoscope.util.Utils.{bucket, getEventId}
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

// scalastyle:off
class FlowInterpreter(
  env: Environment,
  manager: ActorRef,
  event: FlowEvent,
  traceContext: Map[String, TraceVariable],
  config: Config
) extends Actor with Logging {

  import context.dispatcher
  import env._

  import scala.collection.JavaConversions._

  implicit val builtIn = getBuiltIn()

  lazy val flowGraph = getFlowGraph()

  // messages handled by actor
  case class Continue(context: Context)

  case class CompositeResult(context: CompositeContext, result: Try[Value])

  case class BatchCompositeResult(context: BatchCompositeContext, result: Try[Value])

  val traceId: String = event.getTraceId
  val eventId: String = event.getEventId

  val isTopicLogEnabled: Boolean = Try(
    config.getBoolean("horoscope.flow-executor.topic-log.enabled")).getOrElse(true)
  val isLogDetailed: Boolean = Try(
    config.getBoolean("horoscope.flow-executor.detailed-log.enabled")).getOrElse(false)

  override def preStart(): Unit = {
    self ! Continue(Main)
  }

  def receive: Receive = {
    case Continue(context) =>
      Context.continue(context)

    case CompositeResult(composite, result) =>
      composite.done(result)
      Context.continue()

    case BatchCompositeResult(composite, result) =>
      composite.done(result)
      Context.continue()
  }

  type Dependencies = Iterable[Context]
  type Handle = PartialFunction[Try[Value], Unit]
  type ExperimentFlow = (Flow, Option[ExperimentPlan])

  def getExperimentFlow(flow: String, plan: Option[ExperimentPlan])(args: Map[String, Value]): ExperimentFlow = {
    if (plan.isDefined) {
      val flowOption = Try(Some(plan.get.flowOptions(flow))).getOrElse(None)
      val targetFlow = Try(flowOption.map(_.replacement).get).getOrElse(flow)
      (getFlowByName(targetFlow), plan)
    } else {
      var targetFlow: String = flow
      var targetPlan: Option[ExperimentPlan] = None
      var found = false
      for (controller <- getController(flow).sortBy(_.priority) if !found) {
        val plan = controller.query(Value(args))
        if (plan.isDefined) {
          val flowOption = Try(Some(plan.get.flowOptions(flow))).getOrElse(None)
          val exptFlow = Try(flowOption.map(_.replacement).get).getOrElse(flow)
          targetFlow = exptFlow
          targetPlan = plan
          found = true
        }
      }
      (getFlowByName(targetFlow), targetPlan)
    }
  }

  def logTopic(topic: Flow.Topic): Unit = {
    TopicLogState.newBuilder
      .withEvent(event).withTopic(topic).withInterpreter(this)
      .build().deduce().foreach(_.log())
  }

  object Main extends Context {
    val instance: FlowInstance.Builder = {
      FlowInstance.newBuilder()
        .setEvent(event)
        .setStartTime(System.currentTimeMillis())
    }

    override lazy val procedure: FlowContext = FlowContext(event, Seq("main"))

    lazy val tokenContext: Map[String, LoadTokenContext] = if (event.hasToken) {
      Map("@#" -> new LoadTokenContext(event.getToken))
    } else {
      Map.empty[String, LoadTokenContext]
    }

    lazy val logContext: LoadLogContext = new LoadLogContext(event)

    override protected def execute(): Dependencies = {
      try {
        after(tokenContext) {
          if (procedure.isDone && logContext.isDone) {
            done()
          } else if (procedure.isDone) {
            waits(logContext)
          } else {
            waits(procedure)
          }
        }
      } catch {
        case e: Throwable =>
          println(s"main procedure execute failed, ${e.getMessage}")
          done()
      }
    }


    private def collect(procedure: FlowContext): Unit = {
      procedure.collect()
      for (child <- procedure.children) {
        collect(child)
      }
    }

    private def updateLocalTrace(): Unit = {
      val deleteKeys = Log.callbackTokens ++ Log.expiredLogIds
      for (key <- deleteKeys) {
        Trace.deletes += key
        Trace.variables -= key
      }
      instance.getTokenBuilderList.asScala.foreach { builder =>
        val token: String = if (builder.getToken.startsWith("#")) {
          builder.getToken
        } else {
          "#" + builder.getToken
        }
        Trace.variables += (token -> TraceVariable.newBuilder()
          .setValue(Value(Binary(builder.build().toByteArray)).as[FlowValue])
          .build())
      }

      instance.getBackwardList.asScala.foreach { builder =>
        val triggerId = if (builder.getLogId.startsWith("&")) {
          builder.getLogId
        } else {
          "&" + builder.getLogId
        }
        Trace.variables += (triggerId -> TraceVariable.newBuilder()
          .setValue(Value(Binary(builder.toByteArray)).as[FlowValue])
          .build())
      }
    }

    private def log(procedure: FlowContext): Unit = {
      if (isTopicLogEnabled) {
        // 1.collect and register flow variables
        collect(procedure)

        // 2.log topics
        for ((name, topic) <- procedure.flow.totalTopics) {
          logTopic(topic)
        }
      }
    }

    override protected def onComplete: Handle = {
      case _: Success[Value] =>
        log(procedure)
        updateLocalTrace()
        instance.setEndTime(System.currentTimeMillis())
        instance.addAllUpdate(Trace.updates.values.toSeq)
        instance.addAllDelete(Trace.deletes)
        manager ! CommitEvent(traceId, eventId, Try(instance.build()), Trace.variables)

      case Failure(cause) =>
        instance.setEndTime(System.currentTimeMillis())
        manager ! CommitEvent(traceId, eventId, Failure(cause), Map.empty)
    }
  }

  class PlaceholderContext(val name: String, val assign: Try[Value])(
    implicit override val procedure: FlowContext
  ) extends Context with FaultRecorder {
    def this(name: String)(implicit procedure: FlowContext) = this(name, Failure(UninitializedFieldError(name)))

    override def execute(): Dependencies = {
      done(assign)
    }

    override protected def onComplete: Handle = super.onComplete orElse {
      case _ =>
    }
  }

  object FlowContext {
    // for include
    def apply(flowName: String, scopes: Seq[String],
      args: Map[String, Context], exptContext: Map[String, Value],
      exptPlan: Option[ExperimentPlan]
    ): FlowContext = {
      val eventIdMap = Map("@event_id" -> Value(eventId))
      val (flow, plan) = getExperimentFlow(flowName, exptPlan)(exptContext ++ eventIdMap)
      val flowOption = Try(Some(plan.get.flowOptions(flowName))).getOrElse(None)
      val exptArgs = Try {
        flowOption.get.params.mapValues(v => Expression(v).apply(Value(exptContext ++ eventIdMap)))
      }.getOrElse(Map[String, Value]())

      new FlowContext(flow, scopes,
        procedure =>
          args ++
            (eventIdMap ++ exptArgs).iterator.map({
              case (name, variable) =>
                val key = if (name.startsWith("@")) name else "@" + name
                (key, new PlaceholderContext(key, Try(Value(variable)))(procedure))
            }).toMap,
        plan
      )
    }

    // for schedule or callback event
    def apply(event: FlowEvent, scopes: Seq[String]): FlowContext = {
      val flowName = event.getFlowName
      val args = event.getArgumentMap.asScala.mapValues(v => Value(v.getValue)) ++
        Map("@event_id" -> Value(eventId))
      val exptContext = event.exptContext ++ args
      val (flow, plan) = getExperimentFlow(flowName, event.exptPlan)(exptContext)
      val flowOption = Try(Some(plan.get.flowOptions(flowName))).getOrElse(None)
      val exptArgs = Try {
        flowOption.get.params.mapValues({ v => Expression(v).apply(Value(exptContext)) })
      }.getOrElse(Map[String, Value]())

      new FlowContext(
        flow, scopes,
        procedure => (args ++ exptArgs).iterator.map({
          case (name, variable) =>
            val key = if (name.startsWith("@")) name else "@" + name
            (key, new PlaceholderContext(key, Try(variable))(procedure))
        }).toMap ++ Main.tokenContext,
        plan
      )
    }

    implicit class ExperimentHelper(event: FlowEvent) {
      def exptContext: Map[String, Value] = {
        val experiment = Try(Some(event.getExperiment)).getOrElse(None)
        experiment.map({ p =>
          p.getDependencyMap.asScala.mapValues(Value(_))
        }).getOrElse(Map[String, Value]()).toMap
      }

      def exptPlan: Option[ExperimentPlan] = {
        if (event.hasExperiment) {
          val experiment = event.getExperiment
          val plans = experiment.getPlanList.asScala.map(p => {
            val args = p.getArgumentMap.asScala.toMap.mapValues(_.getText)
            p.getOriginal -> FlowOption(p.getReplacement, args)
          }).toMap
          Some(ExperimentPlan(experiment.getName, experiment.getGroup, plans))
        } else {
          None
        }
      }
    }
  }

  class FlowContext(
    val flow: Flow, val scopes: Seq[String], alias: FlowContext => Map[String, Context],
    val plan: Option[ExperimentPlan]
  ) extends Context {

    implicit override def procedure: FlowContext = this

    val args: Map[String, Context] = alias(this)

    val nodes: Array[Context] = flow.nodes.view.map(newContext).toArray

    val children: ListBuffer[FlowContext] = ListBuffer.empty

    val choices: ListBuffer[String] = ListBuffer.empty

    lazy val logDeps: Set[String] = {
      flow.logVariables.view.filter(_.flow == flow.name)
        .flatMap(_.expression.get.references.map(_.identifier)).toSet
    }

    def collect(): Unit = {
      if (!log.getFaultMap.asScala.keySet.exists(_.startsWith("-token-fault->"))) {
        for ((name, argument) <- flow.arguments if argument.isReady) {
          val argName = if (name.startsWith("@")) name else "@" + name
          if (logDeps.contains(argName))
            Log.addVariable(Log.VariableKey(argName, flow.name), scopes, argument.value)
        }

        Log.addChoice(flow.name, procedure.scopes, procedure.choices)
        Log.addFlow(flow.name, procedure.scopes)
      }
    }

    def collectToken(): Unit = {
      if (args.contains("@#")) {
        val token = procedure.args("@#").asInstanceOf[LoadTokenContext].token
        val tokenVal = if (token.startsWith("#")) token else "#" + token
        if (tokenVal.nonEmpty) {
          Log.callbackTokens ::= tokenVal
        }
      }
    }

    val log: FlowInstance.Procedure.Builder = {
      val procedureCount = Main.instance.getProcedureCount
      require(procedureCount <= 128, s"procedure count exceeds $procedureCount")

      val procedure = Main.instance.addProcedureBuilder()
        .setFlowId(flow.id)
        .setFlowName(flow.name)
        .addAllScope(scopes)
        .setStartTime(System.currentTimeMillis())

      if (plan.isDefined) {
        val value = plan.get
        val experiment = Experiment.newBuilder()
          .setName(value.name).setGroup(value.group)
        procedure.setExperiment(experiment)
      }

      procedure
    }

    override protected def execute(): Dependencies = {
      using(flow.terminator) { _ =>
        done()
      }
    }

    override protected def onComplete: Handle = {
      case _ =>
        collectToken()
        for ((name, argument) <- flow.arguments if argument.isReady) {
          log.putArgument(name, argument.value.as[FlowValue])
        }
        log.setEndTime(System.currentTimeMillis())
    }

    def newContext(node: Node): Context = node match {
      case Placeholder(name) => args.getOrElse(name, new PlaceholderContext(name))
      case include: Include => new IncludeContext(include)
      case schedule: Schedule => new ScheduleContext(schedule)
      case subscribe: Subscribe => new SubscribeContext(subscribe)
      case callback: Callback => new CallbackContext(callback)
      case load: Load => new LoadContext(load)
      case update: Update => new UpdateContext(update)
      case terminator: Terminator => new BlockContext(terminator)
      case choice: Choice => new ChoiceContext(choice)
      case node: Switch => new SwitchContext(node)
      case evaluate: Evaluate => new EvaluateContext(evaluate)
      case composite: Composite if composite.isBatch => new BatchCompositeContext(composite)
      case composite: Composite => new CompositeContext(composite)
    }

  }

  trait ExperimentContext extends Context {

    def flow: String

    def procedure: FlowContext

    // 计算实验所有的可能依赖项
    // 除了dependency之外, 还应该包含args
    lazy val references: Map[String, Expression] = if (procedure.plan.isEmpty) {
      getController(flow).flatMap(_.dependency).toMap
    } else if (procedure.plan.get.flowOptions.contains(flow)) {
      val flowPlan = procedure.plan.get.flowOptions(flow)
      val params = flowPlan.params
      params.values.flatMap(v => Expression(v).references).map(ref => ref.name -> ref).toMap
    } else {
      Map()
    }

    lazy val contexts: Map[String, Context] = {
      procedure.nodes.flatMap {
        case l: LoadContext => Some((l.load.name, l))
        case u: UpdateContext => Some((u.update.name, u))
        case p: PlaceholderContext => Some((p.name, p))
        case c: CompositeContext => Some((c.name, c))
        case bc: BatchCompositeContext => Some((bc.name, bc))
        case _ => None
      }.toMap ++ procedure.flow.variables.mapValues(nodeToContext)
    }

    lazy val exptDeps: Map[String, Context] = {
      (references - "@event_id").values.flatMap({ expression =>
        expression.references.flatMap({ ref =>
          if (contexts.contains(ref.identifier)) {
            Some((ref.identifier, contexts(ref.identifier)))
          } else {
            logging.error(s"expression reference variable: ${ref.identifier} doesn't exist" +
              s" in context of flow: ${flow} ")
            None
          }
        })
      }).toMap
    }

    lazy val exptVariables: Map[String, Value] = references.mapValues(_.apply(Value(exptDeps.mapValues(_.value))))
  }

  class IncludeContext(val include: Include)(
    implicit override val procedure: FlowContext
  ) extends Context with FaultRecorder with ExperimentContext {
    override def name: String = include.scope

    override def flow: String = include.flow

    lazy val tokenContext: Map[String, LoadTokenContext] = include.token.map { t =>
      val token = t.value match {
        case Text(t) => t
        case v: Value => v.toString
      }
      "@#" -> new LoadTokenContext("#" + token)
    }.toMap

    lazy val target: FlowContext = {
      val includeContext = FlowContext.apply(
        include.flow,
        procedure.scopes :+ include.scope,
        include.args.map({ case (name, node) => ("@" + name) -> nodeToContext(node) })
          ++ tokenContext,
        exptVariables,
        procedure.plan
      )
      procedure.children += includeContext
      includeContext
    }

    override protected def execute(): Dependencies = {
      after(exptDeps ++ include.token.map("token" -> nodeToContext(_))) {
        after(tokenContext) {
          if (target.isDone) {
            done()
          } else {
            waits(target)
          }
        }
      }
    }

    override protected def onComplete: Handle = super.onComplete orElse {
      case Success(_) =>
        for ((name, placeholder) <- include.exports if target.flow.variables.contains(name)) {
          if (placeholder.isDone) {
            logging.error(s"placeholder ${placeholder.name} is referenced before including " +
              s"${target.flow.name} in ${procedure.flow.name}")
          }
          procedure.nodes(placeholder.index) = target.nodes(target.flow.variables(name).index)
        }
    }
  }

  class SubscribeContext(val subscribe: Subscribe)(
    implicit override val procedure: FlowContext
  ) extends Context with FaultRecorder with ExperimentContext {
    override def name: String = subscribe.scope

    override def flow: String = subscribe.flow

    lazy val target: FlowContext = {
      val child = FlowContext(
        flow,
        procedure.scopes :+ subscribe.scope,
        subscribe.args.map({ case (name, node) => ("@" + name) -> nodeToContext(node) }),
        exptVariables,
        procedure.plan
      )
      procedure.children += child

      child
    }

    override def execute(): Dependencies = {
      after(exptDeps ++
        subscribe.target.map("target" -> nodeToContext(_)) ++
        subscribe.condition.map("condition" -> nodeToContext(_))) {
        val targetValue = Try(subscribe.target.get.value).getOrElse(Value(eventId))
        if (subscribe.condition.forall(_.value.as[Boolean]) &&
          subscribe.traffic.contains(bucket(targetValue))) {
          if (target.isDone) {
            done()
          } else {
            waits(target)
          }
        } else {
          done()
        }
      }
    }

    override protected def onComplete: Handle = super.onComplete orElse {
      case Success(_) => Unit
    }
  }

  class ScheduleContext(val schedule: Schedule)(
    implicit override val procedure: FlowContext
  ) extends Context with ExperimentContext {

    override def flow: String = schedule.flow

    lazy val builder: FlowEvent.Builder = Main.instance.addScheduleBuilder()

    override protected def execute(): Dependencies = {
      after(exptDeps ++ schedule.token.map("token" -> nodeToContext(_))) {
        using(schedule.args ++ schedule.trace.map("$" -> _)) { args =>
          builder.setEventId(getEventId())
          builder.setTraceId(
            args.get("$").map({
              case Text(text) => text
              case value: Value => value.toString
            }).getOrElse(traceId)
          )
          builder.setFlowName(schedule.flow)
          if (schedule.waitTime != Duration.Zero) {
            builder.setScheduledTimestamp(System.currentTimeMillis() + schedule.waitTime.toMillis)
          }
          for ((name, argument) <- args if schedule.args.contains(name)) {
            builder.putArgument(name, TraceVariable.newBuilder().setValue(argument.as[FlowValue]).build())
          }
          if (schedule.token.isDefined) {
            val tokenValue = schedule.token.get.value match {
              case Text(t) => t
              case v: Value => v.toString
            }
            builder.setToken("#" + tokenValue)
          }
          builder.getParentBuilder
            .setEventId(eventId)
            .setTraceId(traceId)
            .setFlowName(procedure.flow.name)
            .addAllScope(procedure.scopes)
          if (procedure.plan.isDefined) {
            builder.setExperiment(procedure.plan.get.experimentContext)
          }
          for ((name, argument) <- exptVariables) {
            builder.getExperimentBuilder.putDependency(name, argument.as[FlowValue])
          }
          Log.scheduleEvents += builder

          done()
        }
      }
    }
  }

  class CallbackContext(val callback: Callback)(
    implicit override val procedure: FlowContext
  ) extends Context {

    override protected def execute(): Dependencies = {
      using(callback.args ++ Map("token" -> callback.token)) { args =>
        if (args("token") == NULL) {
          done()
        } else {
          // 1. save token context
          val tokenBuilder = TokenContext.newBuilder()
            .setToken('#' + args("token").as[String])
          for ((name, argument) <- args if callback.args.contains(name)) {
            tokenBuilder.putArgument(name, argument.as[FlowValue])
          }
          Main.instance.addToken(tokenBuilder.build())

          // 2.1 new timeout schedule event
          val scheduleBuilder = Main.instance.addScheduleBuilder()
          scheduleBuilder.setEventId(getEventId())
            .setTraceId(traceId)
            .setFlowName(callback.timeoutFlow.get)
            .setScheduledTimestamp(System.currentTimeMillis() + callback.timeout.toMillis)
            .setToken('#' + args("token").as[String])

          // 2.2 inherit experiment plan from parent procedure
          if (procedure.plan.isDefined) {
            scheduleBuilder.setExperiment(procedure.plan.get.experimentContext)
          }

          // 3. register callback flow to help logging
          Log.callbackFlows += callback.flow
          done()
        }
      }
    }
  }

  class BlockContext(val terminator: Terminator)(
    implicit override val procedure: FlowContext
  ) extends Context {
    var waitingNodes: List[Node] = Nil
    // 此处仅计算直接依赖的deps，而间接依赖的optDeps并未在terminator的execute里执行
    // 从而isLazy的配置生效
    var remainingNodes: List[Node] = terminator.deps.toList

    override protected def execute(): Dependencies = {
      while (waitingNodes.nonEmpty && waitingNodes.head.isDone) {
        waitingNodes = waitingNodes.tail
      }

      if (waitingNodes.isEmpty && remainingNodes.nonEmpty) {
        var nextBatch: List[Node] = Nil

        while (remainingNodes.nonEmpty && canAsync(nextBatch, remainingNodes)) {
          val head :: tail = remainingNodes
          if (!head.isDone) {
            nextBatch = head :: nextBatch
          }
          remainingNodes = tail
        }

        waitingNodes = nextBatch.reverse
      }

      if (waitingNodes.isEmpty) {
        done()
      } else {
        waits(waitingNodes)
      }
    }

    def canAsync(batch: List[Node], remains: List[Node]): Boolean = (batch, remains) match {
      case (Nil, _) => true
      case ((_: Variable) :: _, (_: Variable) :: _) => true
      case _ => false
    }
  }

  object Log {
    case class VariableKey(name: String, flow: String)
    // variableKey -> [(scopes, value)]
    val variables: mutable.Map[VariableKey, List[(String, Value)]] = mutable.Map()
    // flow -> [(scopes, choice)]
    val choices: mutable.Map[String, List[(String, Seq[String])]] = mutable.Map()
    // flow -> [scopes]
    val flows: mutable.Map[String, Set[String]] = mutable.Map()

    val scheduleEvents: ListBuffer[FlowEvent.Builder] = ListBuffer.empty

    // help to determine log state
    val callbackFlows: ListBuffer[String] = ListBuffer.empty
    // #token
    var callbackTokens: List[String] = Nil
    // &logId
    var expiredLogIds: List[String] = Nil
    // #token -> tokenContext
    val tokenContext: mutable.Map[String, TokenContext] = mutable.Map()

    def getVariables(flow: String, op: String, scope: String = ""): Map[VariableKey, Value] = {
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

    def getChoice(flow: String, op: String, scope: String = ""): Seq[String] = {
      val scopeMatched = choices.getOrElse(flow, Nil).filter(_._1.startsWith(scope))
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
      val oldValue = variables.getOrElse(key, Nil)
      variables += (key -> ((scopes.mkString(","), value) :: oldValue))
    }

    def addChoice(flow: String, scopes: Seq[String], choice: Seq[String]): Unit = {
      val oldValue = choices.getOrElse(flow, Nil)
      choices += (flow -> ((scopes.mkString(","), choice) :: oldValue))
    }

    def addFlow(flow: String, scopes: Seq[String]): Unit = {
      val oldValue = flows.getOrElse(flow, Set())
      flows += (flow -> (oldValue ++ Set(scopes.mkString(","))))
    }
  }

  object ForwardLog {
    // [topic -> [name -> value]]
    val variables: mutable.Map[String, Map[String, Value]] = mutable.Map()

    // [topic -> [name -> choices]
    val choices: mutable.Map[String, Map[String, List[String]]] = mutable.Map()

    // [topic -> (name, flow)]
    val flows: mutable.Map[String, List[(String, String)]] = mutable.Map()

    def addFlow(topic: String, tag: String, flow: String): Unit = {
      val oldValue = flows.getOrElse(topic, Nil)
      flows += (topic -> ((tag, flow) :: oldValue))
    }

    def addChoice(topic: String, name: String, choice: Seq[String]): Unit = {
      var value = choices.getOrElse(topic, Map())
      value += (name -> choice.toList)
      choices += (topic -> value)
    }

    def addVariable(topic: String, name: String, variable: Value): Unit = {
      var value = variables.getOrElse(topic, Map())
      value += (name -> variable)
      variables += (topic -> value)
    }

    def addContext(forward: ForwardContext): Unit = {
      val topic = forward.getTopicName
      flows += (topic -> forward.getTagMap.asScala.mapValues(_.getText).toList)
      choices += (topic -> forward.getChoiceMap.asScala.mapValues(_.getChoiceList.toList).toMap)
      variables += (topic -> forward.getVariableMap.asScala.mapValues(Value(_)).toMap)
    }

    def toForward(topic: String): ForwardContext = {
      val tags = flows.getOrElse(topic, Nil).toMap.mapValues(Value(_).as[FlowValue]).asJava
      val builder = ForwardContext.newBuilder()
        .setTopicName(topic)
        .putAllVariable(variables.getOrElse(topic, Map()).mapValues(_.as[FlowValue]))
        .putAllTag(tags)
      val chooses = choices.getOrElse(topic, Map())
        .mapValues(c => Choose.newBuilder().addAllChoice(c).build())
      builder.putAllChoice(chooses)
      builder.build()
    }
  }

  object BackwardLog {
    // logKey -> name -> value
    val variables: mutable.Map[LogTriggerKey, Map[String, Value]] = mutable.Map()
    // logKey -> name -> choices
    val choices: mutable.Map[LogTriggerKey, Map[String, Seq[String]]] = mutable.Map()
    // logKey -> [(name, flowName)]
    val flows: mutable.Map[LogTriggerKey, List[(String, String)]] = mutable.Map()
    // logKey -> [flow]
    val dependencyFlows: mutable.Map[LogTriggerKey, Set[String]] = mutable.Map()

    def addVariable(key: LogTriggerKey, name: String, variable: Value): Unit = {
      var value = variables.getOrElse(key, Map())
      // 保证就近原则
      if (!value.contains(name)) {
        value += (name -> variable)
        variables += (key -> value)
      }
    }

    def addFlow(key: LogTriggerKey, tag: String, flow: String): Unit = {
      val oldValue = flows.getOrElse(key, Nil)
      flows += (key -> ((tag, flow) :: oldValue))
      removeDependencyFlows(key, Set(flow))
    }

    def setDependencyFlows(key: LogTriggerKey, flows: Set[String]): Unit = {
      if (!dependencyFlows.contains(key)) {
        dependencyFlows += (key -> flows)
      }
    }

    def removeDependencyFlows(key: LogTriggerKey, flows: Set[String]): Unit = {
      val value = dependencyFlows.getOrElse(key, Set())
      dependencyFlows += (key -> (value -- flows))
    }

    def addChoice(key: LogTriggerKey, name: String, choice: Seq[String]): Unit = {
      var value = choices.getOrElse(key, Map())
      // 保证就近原则
      if (!value.contains(name)) {
        value += (name -> choice)
      }
      choices += (key -> value)
    }

    def addContext(key: LogTriggerKey, context: BackwardContext): Unit = {
      flows += (key -> context.getTagMap.asScala.mapValues(_.getText).toList)
      choices += (key -> context.getChoiceMap.asScala.mapValues(_.getChoiceList.asScala).toMap)
      variables += (key -> context.getVariableMap.mapValues(Value(_)).toMap)
      dependencyFlows += (key -> context.getDependencyFlowList.toSet)
    }

    def toBackward(key: LogTriggerKey): BackwardContext = {
      val builder = BackwardContext.newBuilder()
        .setTopicName(key.topicName)
        .setLogId(key.logId)
        .setEventId(key.eventId)
        .putAllVariable(variables.getOrElse(key, Map()).mapValues(_.as[FlowValue]))
      val chooses = choices.getOrElse(key, Map.empty)
        .mapValues(c => Choose.newBuilder().addAllChoice(c).build())
      builder.putAllChoice(chooses)
        .putAllTag(flows.getOrElse(key, Nil).toMap.mapValues(Value(_).as[FlowValue]).asJava)
        .addAllDependencyFlow(dependencyFlows.getOrElse(key, Set()).asJava)
      builder.build()
    }

    def toTopic(key: LogTriggerKey, topic: Flow.Topic): FlowInstance.Topic = {
      val namingMap = topic.tagFields.map(r => (r.flow, r.name)).toMap
      val builder = FlowInstance.Topic.newBuilder()
        .setTraceId(traceId)
        .setTopicName(key.topicName)
        .setLogId(key.logId.substring(1))
        .setTimestamp(System.currentTimeMillis())

      // 只记录收集到的字段, 未收集到的字段不记录
      val variableFields = variables.getOrElse(key, Map())
      val choiceFields = choices.getOrElse(key, Map()).mapValues(Value(_))
      val triggeredTags = flows.getOrElse(key, Nil).map(r => (r._1, Value(true))).toMap
      val unTriggeredTags = dependencyFlows.getOrElse(key, Set()).map(r => (namingMap(r), Value(false))).toMap
      builder.putAllField(
        (variableFields ++ choiceFields ++ triggeredTags ++ unTriggeredTags).mapValues(_.as[FlowValue])
      ).build()
    }
  }

  object Trace {
    var variables: Map[String, TraceVariable] = traceContext

    lazy val load: Future[Unit] = if (traceContext.nonEmpty) {
      Future.successful(Unit)
    } else {
      val future = getTraceContext(traceId, Array.empty).map(results => {
        variables = Map(
          // make sure traceContext will not be empty
          "$" -> TraceVariable.newBuilder().setValue(Value(traceId).as[FlowValue]).build()
        ) ++ results.mapValues({
          case variable: TraceVariable => variable
          case builder: TraceVariable.Builder => builder.build()
        })
      })

      future onFailure {
        case cause: Throwable =>
          manager ! CommitEvent(traceId, eventId, Failure(cause), Map.empty)
          Map.empty
      }

      future
    }

    val updates: mutable.Map[String, TraceVariable] = mutable.Map.empty
    val loads: mutable.Set[String] = mutable.Set.empty
    val deletes: mutable.Set[String] = mutable.Set.empty
  }

  class LoadContext(val load: Load) extends Context {
    override implicit def procedure: FlowContext = Main.procedure

    override protected def execute(): Dependencies = {
      if (Trace.load.isCompleted) {
        Trace.variables.get(load.name) match {
          case Some(variable) =>
            if (!Trace.loads.contains(load.name)) {
              procedure.log.addLoad(variable)
              Trace.loads += load.name
            }
            done(Value(variable.getValue))
          case _ =>
            done(Failure(UninitializedFieldError(load.name)))
        }
      } else {
        Trace.load onSuccess {
          case _ => self ! Continue(this)
        }
        waits()
      }
    }
  }

  class LoadTokenContext(val token: String) extends Context with FaultRecorder {
    override def procedure: FlowContext = Main.procedure

    private val tokenValue: String = if (token.startsWith("#")) token else "#" + token

    override def execute(): Dependencies = {
      if (Trace.load.isCompleted) {
        Trace.variables.get(tokenValue) match {
          case Some(t) =>
            val tokenContext = TokenContext.parseFrom(t.getValue.getBinary)
            Log.tokenContext += (tokenValue -> tokenContext)
            val args = tokenContext.getArgumentMap.asScala.toMap.mapValues(Value(_))
            done(Value(args))
          case None =>
            done(Failure(new IllegalArgumentException(s"token context of ${tokenValue} was expired ")))
        }
      } else {
        Trace.load onSuccess {
          case _ => self ! Continue(this)
        }
        waits()
      }
    }

    override def name: String = "-token-fault->" + tokenValue

    override protected def onComplete: Handle = super.onComplete orElse {
      case Success(_) => Unit
    }
  }

  class LoadLogContext(event: FlowEvent) extends Context {
    override def procedure: FlowContext = Main.procedure

    override protected def execute(): Dependencies = {
      if (Trace.load.isCompleted) {
        // 1.load backward context by trigger from event and token context
        for (trigger <- event.getTriggerList ++ Log.tokenContext.values.map(_.getTrigger)) {
          val logId = if (trigger.getLogId.startsWith("&")) {
            trigger.getLogId
          } else {
           "&" + trigger.getLogId
          }
          val logKey = LogTriggerKey(trigger.getLogId, trigger.getTopicName, trigger.getEventId)
          Trace.variables.get(logId).foreach { traceVariable =>
            BackwardLog.addContext(logKey, BackwardContext.parseFrom(traceVariable.getValue.getBinary))
          }
        }

        // 2.load forward log
        // 2.1 forward context from event
        if (event.hasForward) ForwardLog.addContext(event.getForward)

        // 2.2 forward context from callback timeout event
        for (token <- Log.callbackTokens) {
          val tokenValue = if (token.startsWith("#")) token else "#" + token
          Trace.variables.get(tokenValue) match {
            case Some(t) => {
              ForwardLog.addContext(TokenContext.parseFrom(t.getValue.getBinary).getForward)
            }
            case None => Unit
          }
        }
        done()
      } else {
        Trace.load.onSuccess {
          case _ =>
            Context.continue(this)
        }
        waits()
      }
    }
  }

  class UpdateContext(val update: Update)(
    implicit override val procedure: FlowContext
  ) extends Context {
    override protected def execute(): Dependencies = {
      if (Trace.load.isCompleted) {
        using(update.value) { variable =>
          val traceVariable = TraceVariable.newBuilder()
          traceVariable.setValue(variable.as[FlowValue])
          traceVariable.getReferenceBuilder
            .setEventId(eventId)
            .setName(update.name)
            .addAllScope(procedure.scopes)
            .setFlowName(procedure.flow.name)

          val delta = update.name -> traceVariable.build()
          Trace.variables += delta
          if (!update.isTransient) Trace.updates += delta
          Trace.loads -= update.name

          done(variable)
        }
      } else {
        Trace.load onSuccess {
          case _ => self ! Continue(this)
        }
        waits()
      }
    }
  }

  class ChoiceContext(val choice: Choice)(
    implicit override val procedure: FlowContext
  ) extends Context {
    var action: Option[Terminator] = None

    override protected def execute(): Dependencies = {
      waitAction() orElse waitPrefer() getOrElse makeChoice()
    }

    def waitAction(): Option[Dependencies] = action.map(
      using(_) { _ =>
        done(Text(choice.name))
      }
    )

    def waitPrefer(): Option[Dependencies] = choice.prefer.map(
      using(_) {
        case text: Text => done(text)
        case _ => makeChoice()
      }
    )

    def makeChoice(): Dependencies = {
      if (!choice.condition.isDone) {
        waits(choice.condition)
      } else if (Try(choice.condition.value.as[Boolean]).getOrElse(false)) {
        procedure.log.addChoice(choice.name)
        procedure.choices += choice.name
        action = Some(choice.action)
        waits(action)
      } else {
        done()
      }
    }
  }

  class SwitchContext(val switch: Switch)(
    implicit override val procedure: FlowContext
  ) extends Context {
    override protected def execute(): Dependencies = {
      using(switch.by) { choice =>
        switch.candidates.get(choice.as[String]).map(
          using(_) { candidate =>
            done(candidate)
          }
        ).getOrElse(done(NULL))
      }
    }
  }

  trait LogContext extends Context {
    def procedure: FlowContext

    def flow: String = procedure.flow.name

    def name: String

    def logAssign(node: Variable, value: Value): Unit = {
      if (isTopicLogEnabled && procedure.logDeps.contains(name)) {
        Log.addVariable(Log.VariableKey(name, flow), procedure.scopes, value)
      }

      if (isFlowLogDetailed && !name.startsWith("-") && !node.isTransient) {
        procedure.log.putAssign(name, value.as[FlowValue])
      }
    }
  }

  class EvaluateContext(val evaluate: Evaluate)(
    implicit override val procedure: FlowContext
  ) extends Context with FaultRecorder with LogContext {
    override def name: String = evaluate.name

    override protected def execute(): Dependencies = {
      try {
        using(evaluate.args) { args =>
          val result = evaluate.expression(Value(args))
          done(result)
        }
      } catch {
        case _ if evaluate.failover.nonEmpty =>
          using(evaluate.failoverArgs) { args =>
            done(evaluate.failover.get(Value(args)))
          }
      }
    }

    override protected def onComplete: Handle = super.onComplete orElse {
      case Success(value) =>
        logAssign(evaluate, value)
    }
  }

  class CompositeContext(val composite: Composite)(
    implicit override val procedure: FlowContext
  ) extends Context with FaultRecorder with LogContext {
    override def name: String = composite.name

    val log: Procedure.Composite.Builder = Procedure.Composite.newBuilder()
    var future: Future[Value] = _

    override protected def execute(): Dependencies = {
      using(composite.context) { context =>
        if (future == null) {
          val argument = composite.argument(Value(context)).as[ValueDict]

          log.setCompositor(composite.compositor)
          log.setStartTime(System.currentTimeMillis())
          val beginTime = System.currentTimeMillis()
          future = composite.impl.composite(argument)
          future onComplete { result =>
            val endTime = System.currentTimeMillis()
            log.setEndTime(endTime)
            logInfo(("msg", "composite complete"), ("eventId", eventId),
              ("name", composite.name), ("compositor", composite.compositor),
              ("proc_time", s"${endTime - beginTime}ms"))
            self ! CompositeResult(this, result)
          }
        }

        waits()
      }
    }

    override protected def onComplete: Handle = super.onComplete orElse {
      case Success(value) =>
        procedure.log.putComposite(name, log.build())
        logAssign(composite, value)
    }
  }

  class BatchCompositeContext(val composite: Composite)(
    implicit override val procedure: FlowContext
  ) extends Context with FaultRecorder with LogContext {
    override def name: String = composite.name

    val log: Procedure.Composite.Builder = Procedure.Composite.newBuilder()
    var future: Future[Value] = _

    override protected def execute(): Dependencies = {
      using(composite.context) { context =>
        if (future == null) {
          val table = composite.argument.evaluate(Value(context)).as[Document] match {
            case table: TableView => table
            case doc: Document => doc.table.select(*)
          }

          log.setCompositor(composite.compositor)
          log.setStartTime(System.currentTimeMillis())
          log.setBatchSize(table.size)

          val batchArgs = table.impl.collect({
            case (indices, dict: ValueDict) => indices -> dict
          }).toSeq

          val beginTime = System.currentTimeMillis()
          future = Future.sequence(batchArgs.map({
            case (indices, dict) => composite.impl.composite(dict).map(indices -> _)
          })).map(records => table.update(records).value)

          future onComplete { value =>
            val endTime = System.currentTimeMillis()
            log.setEndTime(endTime)
            logInfo(("msg", "batch composite complete"), ("eventId", eventId),
              ("name", composite.name), ("compositor", composite.compositor),
              ("proc_time", s"${endTime - beginTime}ms"))
            self ! BatchCompositeResult(this, value)
          }
        }

        waits()
      }
    }

    override protected def onComplete: Handle = super.onComplete orElse {
      case Success(value) =>
        procedure.log.putComposite(name, log.build())
        logAssign(composite, value)
    }
  }

  object Context {
    val pendingOperations: mutable.Stack[Context] = mutable.Stack()

    def continue(contexts: Context*): Unit = {
      pendingOperations.pushAll(contexts)
      while (pendingOperations.nonEmpty) {
        pendingOperations.pop().run()
      }
    }
  }

  trait Context {

    import Context._

    private var result: Option[Try[Value]] = None
    private var callbacks: Set[Context] = Set.empty

    implicit def procedure: FlowContext

    @inline final implicit def nodeToContext(node: Node): Context = procedure.nodes(node.index)

    @inline final def value: Value = result.get.get

    @inline final def isDone: Boolean = result.nonEmpty

    @inline final def isReady: Boolean = result.exists(_.isSuccess)

    protected lazy val isFlowLogDetailed: Boolean = isLogDetailed || procedure.flow.isLogEnabled

    protected def execute(): Dependencies = waits()

    protected def onComplete: Handle = {
      case _ =>
    }

    final def run(depth: Int = 0): Unit = if (!isDone) {
      var remainingDependencies: Iterable[Context] = Iterable.empty
      do {
        remainingDependencies = tryExecute(depth)
      } while (remainingDependencies.nonEmpty && remainingDependencies.forall(_.isDone))

      for (dependency <- remainingDependencies if !dependency.isDone) {
        dependency.callbacks += this
      }
    }

    final def tryExecute(depth: Int): Dependencies = {
      try {
        val remainingDependencies = execute().filter(!_.isDone)
        remainingDependencies foreach {
          case dependency: Context if dependency.callbacks.contains(this) =>
          // already notified, just wait
          case include: IncludeContext =>
            // avoid keep thread too busy, allowing other trace to run
            self ! Continue(include)
          case dependency: Context =>
            if (depth < 16) {
              dependency.run(depth + 1)
            } else {
              // avoid stack overflow
              Context.pendingOperations.push(dependency)
            }
        }
        remainingDependencies
      } catch {
        case cause: Throwable =>
          done(Failure(cause))
      }
    }

    final def done(value: Try[Value]): Dependencies = {
      if (!isDone) {
        result = Some(value)
        try {
          onComplete(value)
        } catch {
          case exception: Exception =>
            logging.error(s"fail to complete flow operation", exception)
        }

        callbacks.foreach(pendingOperations.push)
      }
      Nil
    }

    final def done(value: Value = NULL): Dependencies = {
      done(Success(value))
    }

    final def waits(contexts: Context*): Dependencies = {
      contexts
    }

    final def waits(nodes: Iterable[Node]): Dependencies = {
      nodes.map(nodeToContext)
    }

    final def using(node: Node)(f: Value => Dependencies): Dependencies = {
      if (node.isReady) {
        f(node.value)
      } else if (node.isDone) {
        throw new IllegalArgumentException(node.toString)
      } else {
        waits(node)
      }
    }

    final def using(nodes: Map[String, Node])(f: Map[String, Value] => Dependencies): Dependencies = {
      if (nodes.values.forall(_.isDone)) {
        val failures = nodes.filter(!_._2.isReady)
        if (failures.nonEmpty) {
          throw new IllegalArgumentException(failures.keys.mkString(", "))
        } else {
          f(nodes.mapValues(_.value))
        }
      } else {
        waits(nodes.values)
      }
    }

    final def after(contexts: Map[String, Context])(f: => Dependencies): Dependencies = {
      if (contexts.values.forall(_.isDone)) {
        val failures = contexts.filter(!_._2.isReady)
        if (failures.nonEmpty) {
          throw new IllegalArgumentException(failures.keys.mkString(", "))
        } else {
          f
        }
      } else {
        contexts.values
      }
    }
  }

  trait FaultRecorder extends Context {
    def name: String

    override protected def onComplete: Handle = {
      case Failure(exception) =>
        val cause = exception match {
          case e: ExecutionException => e.getCause
          case _ => exception
        }

        val fault = FlowInstance.Procedure.Fault.newBuilder()
        fault.setCatalog(cause.getClass.getSimpleName)
        if (cause.getMessage != null) fault.setMessage(cause.getMessage) else fault.setMessage("")

        cause match {
          case _: IllegalArgumentException =>
          case _: UninitializedFieldError =>
          case _: SyntaxException =>
          case _: SemanticException =>
          // do not need to log detail when caused by upstream fault

          case _ =>
            val detail: StringWriter = new StringWriter()
            cause.printStackTrace(new PrintWriter(detail))
            fault.setDetail(detail.toString)
        }

        procedure.log.putFault(name, fault.build())
    }
  }

}