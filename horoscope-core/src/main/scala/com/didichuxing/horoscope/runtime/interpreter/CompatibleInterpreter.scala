/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.runtime.interpreter

import java.util.concurrent.ExecutionException

import akka.actor.{Actor, ActorRef}
import com.didichuxing.horoscope.core.Flow
import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowEvent.TokenStatus
import com.didichuxing.horoscope.core.FlowRuntimeMessage._
import com.didichuxing.horoscope.runtime.FlowExecutorImpl.CommitEvent
import com.didichuxing.horoscope.runtime._
import com.didichuxing.horoscope.util.{Logging, Utils}
import com.didichuxing.horoscope.util.Constants.CONTEXT_VAL_FLAG

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

class CompatibleInterpreter(
  flow: Flow, event: FlowEvent, env: Environment, manager: ActorRef, traceContext: Map[String, TraceVariable]
) extends Actor with Logging {
  import Flow._
  import akka.pattern.pipe
  import context.dispatcher
  import env._

  import scala.collection.JavaConversions._

  trait Operation
  object Exit extends Operation
  object BlockDone extends Operation
  case class Wait(nodes: Iterable[Node]) extends Operation
  case class Require(deps: Iterable[Node]) extends Operation
  case class Choose(name: String, action: Option[Terminator] = None) extends Operation
  case class Assign(value: Try[Value]) extends Operation

  // messages handled by actor
  case class LoadResults(results: Map[Load, Try[TraceVariableOrBuilder]])
  case class CompositeResult(composite: Composite, result: Try[Value])
  case class BatchCompositeResult(batchComposite: Composite, result: Try[Value])

  val traceId: String = event.getTraceId
  val eventId: String = event.getEventId

  val instance: FlowInstance.Builder =
    FlowInstance.newBuilder().setFlowId(flow.id).setEvent(event).setStartTime(System.currentTimeMillis())
  val results: Seq[NodeResult] = flow.nodes.map(new NodeResult(_))
  val error: Option[NodeResult] = results.find(_.name == "@error")

  val scope: mutable.Stack[String] = mutable.Stack()
  val pendingNodes: mutable.Stack[Node] = mutable.Stack(flow.terminator)
  val waitingNodes: mutable.Set[Node] = mutable.Set()

  val runningNodes: mutable.SortedSet[Node] = mutable.SortedSet()

  implicit def nodeToResult(node: Node): NodeResult = results(node.index)

  class NodeResult(val node: Node) {
    var isReady: Boolean = false
    var value: Value = NULL

    val name: String = node match {
      case argument: Placeholder => argument.name
      case load: Load => load.name
      case composite: Composite => composite.name
      case evaluate: Evaluate => evaluate.name
      case choice: Choice => choice.name
      case _ => ""
    }

    private var _reference: Option[ValueReference] = None

    lazy val reference: ValueReference = _reference getOrElse {
      assert(isReady)
      ValueReference.newBuilder()
        .setEventId(eventId)
        .setName(name)
        .build()
    }

    def update(token: TokenStatus): Unit = {
      isReady = true
      value = Value(Map(
        "value" -> Value(event.getToken.getValue),
        "owner" -> Value(event.getToken.getOwner)
      ))
      if (token.hasReference) {
        _reference = Some(token.getReference)
      }
    }

    def update(result: Try[TraceVariableOrBuilder]): Unit = {
      isReady = true
      result match {
        case Success(variable) =>
          value = Value(variable.getValue)
          if (variable.hasReference) {
            _reference = Some(variable.getReference)
          }
        case Failure(exception) =>
          error.foreach(r => r.value = r.value.asInstanceOf[ValueDict].updated(name, toValue(exception)))
      }
    }

    def update: PartialFunction[Operation, Unit] = {
      case Assign(Success(result)) =>
        value = result
        isReady = true
        _reference = node match {
          case Switch(_, candidates, by) =>
            candidates.get(by.value.as[String]).map(_.reference)
          case _ => None
        }

      case Assign(Failure(exception)) =>
        isReady = true
        error.foreach(r => r.value = r.value.asInstanceOf[ValueDict].updated(name, toValue(exception)))
        logWarn(("msg", s"fail to assign $name"), ("exception", exception))

      case Choose(name, _) =>
        value = Text(name)
        isReady = true

      case Exit =>
        isReady = true

      case BlockDone =>
        isReady = true

      case _ =>
        isReady = false
    }
  }

  override def preStart(): Unit = {
    val variables = flow.nodes.collect({
      case argument @ Placeholder(name) if name != "@error" => name -> argument
      case load @ Load(name) => name -> load
    }).toMap

    val eventArgs: Map[String, TraceVariable] = traceContext ++ event.getArgumentMap.map({ case (name, variable) =>
      val key = if (name.startsWith("@")) name else "@" + name
      key -> variable
    })
    for ((name, variable) <- variables) {
      eventArgs.get(name) match {
        case Some(message) =>
          variable.update(Success(message))
        case None =>
          if (variable.isInstanceOf[Placeholder]) {
            variable.update(Success(TraceVariable.newBuilder().setValue(NULL.as[FlowValue])))
          }
      }
    }

    if (event.hasToken) {
      variables.get(event.getToken.getName).foreach(_.update(event.getToken))
    }

    error.foreach(_.isReady = true)
    error.foreach(_.value = new SimpleDict(Map.empty[String, Value]))

    mainLoop()
  }

  override def receive: Receive = {
    case LoadResults(results) =>
      for ((load, result) <- results) {
        load.update(result)
        waitingNodes -= load
      }

      mainLoop()

    case CompositeResult(composite, result) =>
      val operation = Assign(result)
      composite.update(operation)
      log(composite -> operation)
      waitingNodes -= composite

      mainLoop()

    case BatchCompositeResult(composite, result) =>
      val operation = Assign(result)
      composite.update(operation)
      log(composite -> operation)
      waitingNodes -= composite

      mainLoop()
  }

  def mainLoop(): Unit = {
    while (waitingNodes.isEmpty && pendingNodes.nonEmpty) {
      val node = pendingNodes.pop()
      if (!node.isReady) {
        val operation = process(node)
        node.update(operation)
        log(node -> operation)

        operation match {
          case Wait(deps) =>
            waitingNodes ++= deps
          case Require(deps) =>
            pendingNodes.push(node)
            deps.toSeq.sortBy(-_.index).foreach(pendingNodes.push)
          case Choose(name, Some(action)) =>
            scope.push(name)
            pendingNodes.push(action)
          case BlockDone if scope.nonEmpty =>
            scope.pop()
          case Exit =>
            pendingNodes.clear()
          case _ =>
        }
      }
    }

    if (pendingNodes.isEmpty) {
      commit()
    }
  }

  def commit(): Unit = {
    val result = Try {
      assert(shouldAccept(traceId), s"executor do not accept trace $traceId any more")
      instance.setEndTime(System.currentTimeMillis())
      instance.build()
    }

    if (result.isSuccess) {
      val loads = results.filter(result =>
        result.isReady && result.node.isInstanceOf[Load]
      )

      val assigns = results.filter(result =>
        result.isReady && (result.node.isInstanceOf[Evaluate] || result.node.isInstanceOf[Composite])
      )

      val updates = (loads ++ assigns).filter(_.name.startsWith(CONTEXT_VAL_FLAG)).map({ result =>
        result.name -> {
          TraceVariable.newBuilder()
            .setValue(Try(result.value.as[FlowValue]).getOrElse(NULL.as[FlowValue]))
            .setReference(result.reference)
            .build()
        }
      })

      manager ! CommitEvent(traceId, eventId, result, traceContext ++ updates)
    } else {
      // clear cache
      manager ! CommitEvent(traceId, eventId, result, Map.empty)
    }
  }

  // scalastyle:off
  def process: PartialFunction[Flow.Node, Operation] = {
    case _: Load =>
      val missingLoads = flow.nodes collect { case load: Load if !load.isReady => load }
      getTraceContext(traceId, missingLoads.map(_.name).toArray).map(variables => {
        val pairs = missingLoads.map(load =>
          load -> Try(variables(load.name))
        )
        LoadResults(pairs.toMap)
      }).recover({
        case exception: Throwable =>
          LoadResults(missingLoads.map(load => load -> Failure(exception)).toMap)
      }).pipeTo(self)

      Wait(missingLoads)

    case evaluate: Evaluate =>
      using(evaluate.args) { values =>
        Assign(Try(evaluate.expression(Value(values))))
      }

    case composite: Composite if composite.isBatch =>
      using(composite.context) { context =>
        try {
          val batchArgs = composite.argument(Value(context)).as[Seq[ValueDict]]
          Future.sequence(batchArgs.map(composite.impl.composite)) onComplete { result =>
            self ! BatchCompositeResult(composite, result.map(Value(_)))
          }
        } catch {
          case e: Exception => self ! BatchCompositeResult(composite, Failure(e))
        }
        Wait(composite :: Nil)
      }

    case composite: Composite =>
      using(composite.context) { context =>
        try {
          val args = composite.argument(Value(context)).as[ValueDict]
          logDebug(("msg", "composite begin"), ("eventId", eventId),
            ("name", composite.name), ("compositor", composite.compositor))
          val beginTime = System.currentTimeMillis()
          composite.impl.composite(args) onComplete { result =>
            val endTime = System.currentTimeMillis()
            logInfo(("msg", "composite complete"), ("eventId", eventId),
              ("name", composite.name), ("compositor", composite.compositor),
              ("proc_time", s"${endTime - beginTime}ms"))
            self ! CompositeResult(composite, result)
          }
        } catch {
          case e: Exception => self ! CompositeResult(composite, Failure(e))
        }
        Wait(composite :: Nil)
      }

    case Switch(_, candidates, by) =>
      using(by) {
        case Text(choice) =>
          if (candidates.contains(choice)) {
            using(candidates(choice)) { value =>
              Assign(Success(value))
            }
          } else {
            Assign(Success(NULL))
          }
      }

    case choice @ Choice(name, condition) =>
      def makeChoice(): Operation = using(condition) { value =>
        if (value.as[Boolean]) {
          Choose(name, Some(choice.action))
        } else {
          Choose("", None)
        }
      }
      choice.prefer.map(using(_){
        case Text("") => makeChoice()
        case Text(name) => Choose(name, None)
      }).getOrElse(makeChoice())

    case terminator: Terminator =>
      requirements(terminator.deps).getOrElse(BlockDone)

    case goto: Schedule =>
      requirements(goto.args.values).getOrElse(Exit)
  }

  def log: PartialFunction[(Node, Operation), Unit] = {
    case (node @ Evaluate(name, _, args), Assign(result)) =>
      val assign = instance.addAssignBuilder()
      assign.setName(name)
      assign.setValue(Try(node.value.as[FlowValue]).getOrElse(NULL.as[FlowValue]))
      result match {
        case Failure(exception) =>
          assign.setError(toValue(exception).as[FlowValue])
        case _ =>
      }
      scope.headOption.foreach(assign.setChoice)
      args.values.map(_.reference).foreach(assign.addDependency)

    case (node @ Composite(name, compositor, args), Assign(result)) =>
      val assign = instance.addAssignBuilder()
      assign.setName(name)
      assign.setValue(node.value.as[FlowValue])
      result match {
        case Failure(exception) =>
          assign.setError(toValue(exception).as[FlowValue])
        case _ =>
      }
      scope.headOption.foreach(assign.setChoice)

      assign.setCompositor(compositor)
      for ((key, node) <- args) {
        assign.addDependency(node.reference)
        assign.putCompositeArgument(key, node.reference)
      }

    case (choice: Choice, Choose(name, Some(_))) =>
      def predicates(c: Choice): Seq[ValueReference] = {
        c.prefer.toSeq.flatMap(predicates) :+ c.condition.reference
      }

      val choose = instance.addChooseBuilder()
      choose.setChoice(name)
      scope.headOption.foreach(choose.setParent)
      predicates(choice).foreach(choose.addPredicate)

    case (node @ Schedule(_, flow, _), Exit) =>
      val goto = instance.getGotoBuilder
      goto.setEventId(Utils.getEventId())
      goto.setTraceId(traceId)
      goto.setFlowName(flow)
      if (node.waitTime != Duration.Zero) {
        goto.setScheduledTimestamp(System.currentTimeMillis() + node.waitTime.toMillis)
      }

      for ((name, node) <- node.args) {
        goto.putArgument("@" + name, {
          TraceVariable.newBuilder()
            .setValue(node.value.as[FlowValue])
            .setReference(node.reference)
            .build()
        })
      }

      scope.headOption.foreach(goto.setChoice)

    case _ =>
  }

  def requirements(nodes: Iterable[Node]): Option[Require] = {
    val unreadyNodes = nodes.filter(!_.isReady)
    if (unreadyNodes.isEmpty) {
      None
    } else {
      Some(Require(unreadyNodes))
    }
  }

  def using(node: Node)(f: Value => Operation): Operation = {
    if (node.isReady) {
      f(node.value)
    } else {
      Require(node :: Nil)
    }
  }

  def using(nodes: Map[String, Node])(f: Map[String, Value] => Operation): Operation = {
    requirements(nodes.values).getOrElse(f(nodes.mapValues(_.value)))
  }

  def toValue: PartialFunction[Throwable, Value] = {
    case e: ExecutionException =>
      toValue(e.getCause)
    case t: Throwable =>
      Value(Map(
        "class" -> t.getClass.getSimpleName,
        "message" -> (if (t.getMessage == null) "null" else t.getMessage)
      ))
  }
}

