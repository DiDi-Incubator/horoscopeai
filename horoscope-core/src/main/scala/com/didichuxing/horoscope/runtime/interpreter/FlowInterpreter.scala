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
import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowInstance.Procedure
import com.didichuxing.horoscope.core.FlowRuntimeMessage._
import com.didichuxing.horoscope.dsl.{SemanticException, SyntaxException}
import com.didichuxing.horoscope.runtime.FlowExecutorImpl.CommitEvent
import com.didichuxing.horoscope.runtime._
import com.didichuxing.horoscope.util.Utils.{bucket, getEventId}
import com.didichuxing.horoscope.util.Logging
import com.typesafe.config.Config

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}
import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowEvent.Parent
import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowEvent.{Choice => ChoiceMessage}
import com.didichuxing.horoscope.runtime.expression.Expression
import com.didichuxing.horoscope.runtime.experiment.ExperimentController.ExperimentChoice

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

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

  // messages handled by actor
  case class Continue(context: Context)

  case class CompositeResult(context: CompositeContext, result: Try[Value])

  case class BatchCompositeResult(context: BatchCompositeContext, result: Try[Value])

  val traceId: String = event.getTraceId
  val eventId: String = event.getEventId

  val isLogRedirected: Boolean = Try(config.getBoolean("horoscope.flow-executor.log.redirected")).getOrElse(true)
  val isLogDetailed: Boolean = Try(config.getBoolean("horoscope.flow-executor.log.detailed")).getOrElse(false)

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
  type ExperimentFlow = (Flow, Option[ExperimentChoice])

  def getExperimentFlow(flow: String)(args: ValueDict): ExperimentFlow = {
    val controller = getController(flow)
    if (controller.isDefined) {
      val experimentInfo = controller.get.evaluate(Value(args))
      (getFlowByName(experimentInfo.flow), Some(experimentInfo))
    } else {
      (getFlowByName(flow), None)
    }
  }

  object Main extends Context {
    val instance: FlowInstance.Builder = {
      FlowInstance.newBuilder()
        .setEvent(event)
        .setStartTime(System.currentTimeMillis())
    }

    override lazy val procedure: FlowContext = FlowContext.apply(
      event.getFlowName, Seq("main"), event.getArgumentMap, Try(Some(event.getParent)).getOrElse(None)
    )

    override protected def execute(): Dependencies = {
      if (procedure.isDone) {
        done()
      } else {
        waits(procedure)
      }
    }

    // load variables and choices inherited from parent flow
    private def loadParent(parent: Option[Parent]): Unit = {
      parent.foreach { p =>
        p.getVariableList.asScala.foreach { variable =>
          val variableName = variable.getReference.getName
          val flow = variable.getReference.getFlowName
          val value = Value(variable.getValue)
          Log.addVariable(Log.VariableKey(variableName, flow), Nil, value)
        }

        p.getChoiceMap.asScala.foreach { pair =>
          val (flow, choice) = pair
          Log.addChoice(flow, Nil, choice.getChoiceList)
        }
      }
    }

    // collect log variables and choices
    private def collect(procedure: FlowContext): Unit = {
      procedure.collect()
      for (child <- procedure.children) {
        collect(child)
      }
    }

    private def doLog(procedure: FlowContext): Unit = {
      procedure.doLog()
      for (child <- procedure.children) {
        doLog(child)
      }
    }

    private def log(procedure: FlowContext): Unit = {
      if (isLogRedirected) {
        // 1.load parent variables and choices
        loadParent(procedure.parent)
        // 2.collect and register flow variables
        collect(procedure)
        // 3.log flow assigns and choices
        doLog(procedure)
      }
    }

    override protected def onComplete: Handle = {
      case _: Success[Value] =>
        instance.setEndTime(System.currentTimeMillis())
        instance.addAllUpdate(Trace.updates.values.toSeq)
        log(procedure)
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
    def apply(flowName: String, scopes: Seq[String],
      args: Map[String, Context], experimentContext: ValueDict): FlowContext = {
      val (flow, exptChoice) = getExperimentFlow(flowName)(experimentContext.updated("@event_id", Value(eventId)))
      val experimentArgs = Try(exptChoice.get.params).getOrElse(Map())

      new FlowContext(flow, scopes,
        procedure => (args ++ experimentArgs.iterator.map({
          case (name, variable) =>
            val key = if (name.startsWith("@")) name else "@" + name
            (key, new PlaceholderContext(key, Try(Value(variable)))(procedure))
        }).toMap ++ Map(
          "@event_id" -> new PlaceholderContext("@event_id", Try(Value(eventId)))(procedure)
        )),
        exptChoice
      )
    }

    def apply(flowName: String, scopes: Seq[String],
      args: java.util.Map[String, TraceVariable], parent: Option[Parent]): FlowContext = {
      val exptContext = Value(parent.exptContext ++
        args.mapValues(v => Value(v.getValue)) ++
        Map("@event_id" -> Value(eventId))
      )
      val (flow, exptChoice) = getExperimentFlow(flowName)(exptContext)
      val exptArgs = Try(exptChoice.get.params).getOrElse(Map())

      new FlowContext(
        flow, scopes,
        procedure => ((args.mapValues(v => Value(v.getValue)) ++ exptArgs).iterator.map({
          case (name, variable) =>
            val key = if (name.startsWith("@")) name else "@" + name
            (key, new PlaceholderContext(key, Try(variable))(procedure))
        }).toMap ++ Map(
          "@event_id" -> new PlaceholderContext("@event_id", Try(Value(eventId)))(procedure)
        )),
        exptChoice, parent
      )
    }

    implicit class ParentHelper(parent: Option[FlowEvent.Parent]) {
      def exptContext: Map[String, Value] = {
        val experimentContext = parent.map({ p =>
          p.getExperimentMap.asScala.mapValues(Value(_))
        }).getOrElse(Map[String, Value]())

        experimentContext.toMap
      }
    }
  }

  class FlowContext(
    val flow: Flow, val scopes: Seq[String], alias: FlowContext => Map[String, Context],
    val experimentChoice: Option[ExperimentChoice], val parent: Option[Parent] = None
  ) extends Context {

    implicit override def procedure: FlowContext = this

    val args: Map[String, Context] = alias(this)

    val nodes: Array[Context] = flow.nodes.view.map(newContext).toArray

    val children: ListBuffer[FlowContext] = ListBuffer.empty

    val schedules: ListBuffer[ScheduleContext] = ListBuffer.empty

    val choices: ListBuffer[String] = ListBuffer.empty

    lazy val variableContexts: Map[String, Context] = {
      nodes.flatMap {
        case l: LoadContext => Some(l.load.name, l)
        case u: UpdateContext => Some((u.update.name, u))
        case e: EvaluateContext => Some((e.name, e))
        case p: PlaceholderContext => Some((p.name, p))
        case c: CompositeContext => Some((c.name, c))
        case bc: BatchCompositeContext => Some((bc.name, bc))
        case _ => None
      }
    }.toMap

    def collect(): Unit = {
      flow.logVariables.filter(_.from == flow.name).foreach { l =>
        l.expression.references.foreach { ref =>
          val context = variableContexts.get(ref.identifier)
          if (context.isDefined && context.get.isReady) {
            val value = context.get.value
            Log.addVariable(Log.VariableKey(ref.identifier, l.from), procedure.scopes, value)
          }
        }
      }

      Log.addChoice(flow.name, procedure.scopes, procedure.choices)
    }

    def doLog(): Unit = {
      flow.logVariables.filter(_.to == procedure.flow.name).foreach { f =>
        val context = Log.variables.filter(_._1.flow == f.from).map(r => r._1.name -> r._2)
        val value = Try(Some(f.expression.apply(Value(context)))).getOrElse(None)
        if (value.isDefined) log.putAssign(f.name, value.get.as[FlowValue])
      }

      flow.logChoices.foreach { flow =>
        val flowChoice = Log.choices.getOrElse(flow, Nil).map(choice => s"$flow:$choice")
        flowChoice.foreach(log.addContextChoice)
      }

      schedules.foreach { schedule =>
        val parent = schedule.builder.getParentBuilder
        Log.variables.foreach { record =>
          val (Log.VariableKey(name, flow), value) = record
          val traceVariable = TraceVariable.newBuilder()
          val valueReference = ValueReference.newBuilder().setEventId(eventId).setFlowName(flow).setName(name)
          traceVariable.setReference(valueReference)
          traceVariable.setValue(value.as[FlowValue])
          parent.addVariable(traceVariable.build())
        }

        Log.choices.foreach { record =>
          val (flow, choices) = record
          if (choices.nonEmpty) {
            val builder = ChoiceMessage.newBuilder().addAllChoice(choices.asJava)
            parent.putChoice(flow, builder.build())
          }
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

      if (experimentChoice.isDefined) {
        val exptChoice = experimentChoice.get
        val experiment = FlowInstance.Experiment.newBuilder()
          .setName(exptChoice.name).setGroup(exptChoice.group)
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

  trait ExperimentContext {

    def flow: String

    def procedure: FlowContext

    lazy val expressions: Map[String, Expression] = getController(flow).map(_.dependency).getOrElse(Map())

    lazy val exptDependencies: Map[String, Context] = {
      (expressions - "@event_id").values.flatMap({ expression =>
        expression.references.flatMap({ ref =>
          if (procedure.variableContexts.contains(ref.identifier)) {
            Some((ref.identifier, procedure.variableContexts(ref.identifier)))
          } else {
            logging.error(s"expression reference variable: ${ref.identifier} doesn't exist" +
              s" in context of flow: ${flow} ")
            None
          }
        })
      }).toMap
    }

    lazy val exptVariables: Map[String, Value] = expressions.mapValues(_.apply(Value(exptDependencies.mapValues(_.value))))
  }

  class IncludeContext(val include: Include)(
    implicit override val procedure: FlowContext
  ) extends Context with FaultRecorder with ExperimentContext {
    override def name: String = include.scope
    override def flow: String = include.flow

    lazy val target: FlowContext = {
      val childContext = FlowContext(
          include.flow,
          procedure.scopes :+ include.scope,
          include.args.map({ case (name, node) => ("@" + name) -> nodeToContext(node) }),
          Value(exptVariables)
      )
      procedure.children += childContext

      childContext
    }

    override protected def execute(): Dependencies = {
      after(exptDependencies) {
        if (target.isDone) {
          done()
        } else {
          waits(target)
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
        Value(exptVariables)
      )
      procedure.children += child

      child
    }

    override def execute(): Dependencies = {
      after(exptDependencies ++ subscribe.condition.map("condition" -> nodeToContext(_)) ++
        subscribe.target.map("target" -> nodeToContext(_))) {
        if (subscribe.condition.forall(_.value.as[Boolean]) &&
          subscribe.target.forall(t => subscribe.traffic.contains(bucket(t.value)))) {
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
      after(exptDependencies) {
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

          val parent = builder.getParentBuilder
            .setEventId(eventId)
            .setTraceId(traceId)
            .setFlowName(procedure.flow.name)
            .addAllScope(procedure.scopes)

          for ((name, argument) <- exptVariables) {
            parent.putExperiment(name, argument.as[FlowValue])
          }
          procedure.schedules += this

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
    private var _variables: Map[VariableKey, List[(Seq[String], Value)]] = Map()
    // flow -> [(scopes, choice)]
    private var _choices: Map[String, List[(Seq[String], Seq[String])]] = Map()

    // When a flow is called recursively, only return the last (the longest scopes) procedure
    lazy val variables: Map[VariableKey, Value] = {
      _variables.mapValues(v => v.maxBy(_._1.length)._2)
    }

    lazy val choices: Map[String, Seq[String]] = {
      _choices.mapValues(c => c.maxBy(_._1.length)._2)
    }

    def addVariable(key: VariableKey, scopes: Seq[String], value: Value): Unit = {
      val oldValue = _variables.getOrElse(key, Nil)
      _variables += (key -> ((scopes, value) :: oldValue))
    }

    def addChoice(flow: String, scopes: Seq[String], choice: Seq[String]): Unit = {
      val oldValue = _choices.getOrElse(flow, Nil)
      _choices += (flow -> ((scopes, choice) :: oldValue))
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

  class EvaluateContext(val evaluate: Evaluate)(
    implicit override val procedure: FlowContext
  ) extends Context with FaultRecorder {
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
        if (isFlowLogDetailed && !name.startsWith("-") && !evaluate.isTransient) {
          procedure.log.putAssign(name, value.as[FlowValue])
        }
    }
  }

  class CompositeContext(val composite: Composite)(
    implicit override val procedure: FlowContext
  ) extends Context with FaultRecorder {
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
        if (isFlowLogDetailed && !composite.isTransient) procedure.log.putAssign(name, value.as[FlowValue])
    }
  }

  class BatchCompositeContext(val composite: Composite)(
    implicit override val procedure: FlowContext
  ) extends Context with FaultRecorder {
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
        if (isFlowLogDetailed && !composite.isTransient) procedure.log.putAssign(name, value.as[FlowValue])
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