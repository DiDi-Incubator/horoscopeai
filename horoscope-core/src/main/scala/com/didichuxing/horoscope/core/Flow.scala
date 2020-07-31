/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.core

import com.didichuxing.horoscope.core.FlowDslMessage.FlowDef
import com.didichuxing.horoscope.dsl.{CompatibleBuilder, FlowBuilder}
import com.didichuxing.horoscope.runtime.expression.{BuiltIn, Expression}
import com.didichuxing.horoscope.util.FlowChart

import scala.collection.SortedSet
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.language.implicitConversions

class Flow(val flowDef: FlowDef)(
  val nodes: Seq[Flow.Node],
  val terminator: Flow.Terminator,
  val arguments: Map[String, Flow.Placeholder],
  val variables: Map[String, Flow.Node]
) {
  import scala.collection.JavaConversions._

  def name: String = flowDef.getName

  def id: String = flowDef.getId

  def isCompatible: Boolean = flowDef.getConfigMap.getOrElse("mode", "") == "compatible"

  def chart: FlowChart = new FlowChart(this)
}

object Flow {
  import com.didichuxing.horoscope.core.FlowDslMessage._

  implicit def nodeOrdering: Ordering[Node] = Ordering.by(_.index)

  class FlowBuildException(msg: String, cause: Exception) extends Exception(msg, cause)

  @throws[FlowBuildException]("if fail to parse FlowDef")
  def apply(flowDef: FlowDef)(
    implicit
    buildCompositor: CompositorDef => Compositor,
    builtin: BuiltIn
  ): Flow = {
    val mode = flowDef.getConfigMap.getOrDefault("mode", "")
    val builder: Builder = mode match {
      case "compatible" =>
        new CompatibleBuilder(flowDef)
      case _ =>
        new FlowBuilder(flowDef)
    }

    Builder.current.set(builder)
    try {
      val flow = builder.build()

      for (node <- flow.nodes) {
        for(dep <- node.deps ++ node.optDeps) {
          flow.nodes(dep.index)._descendants += node
        }

        if (node.leader != null) {
          node.leader._followers += node
        }
      }

      flow
    } finally {
      Builder.current.remove()
    }
  }

  case class Terminator(deps: Set[Flow.Node])(
    override val optDeps: Set[Flow.Node]
  ) extends Node {
    // all nodes lead by self
    private[Flow] var _followers: SortedSet[Node] = SortedSet.empty

    def followers: SortedSet[Node] = _followers
  }

  case class Placeholder(name: String) extends Node {
    override val deps: Set[Node] = Set.empty
  }

  case class Load(name: String) extends Node {
    override val deps: Set[Node] = Set.empty
  }

  case class Update(name: String, value: Node) extends Node {
    override val deps: Set[Node] = Set(value)
    override val optDeps: Set[Node] = Set.empty
  }

  case class Evaluate(name: String, expression: Expression, args: Map[String, Node])(
    val failover: Option[Expression] = None, val failoverArgs: Map[String, Node] = Map.empty
  ) extends Node with Variable {
    override val deps: Set[Node] = args.values.toSet
    override val optDeps: Set[Node] = failoverArgs.values.toSet
  }

  case class Composite(name: String, compositor: String, context: Map[String, Node])(
    val impl: Compositor, val argument: Expression, val isBatch: Boolean
  ) extends Node with Variable {
    override val deps: Set[Node] = context.values.toSet

    lazy val expressions: SortedSet[Evaluate] = Node.search(deps)({
      case evaluate: Evaluate => evaluate.deps
    }).asInstanceOf[SortedSet[Evaluate]]
  }

  case class Choice(name: String, condition: Node)(
    val action: Terminator, val prefer: Option[Choice]
  ) extends Node {
    override val deps: Set[Node] = prefer.toSet[Node]

    override val optDeps: Set[Node] = Set(condition, action)

    val entry: Choice = prefer match {
      case None => this
      case Some(choice) => choice.entry
    }

    val seq: Seq[Choice] = prefer.toSeq.flatMap(_.seq) :+ this

    def branch: Seq[Choice] = entry.search({
      case choice: Choice => choice.descendants
    }).toSeq.asInstanceOf[Seq[Choice]]
  }

  case class Switch(
    name: String, candidates: Map[String, Node], by: Choice
  ) extends Node with Variable {
    override val deps: Set[Node] = Set(by)

    override val optDeps: Set[Node] = candidates.values.toSet
  }

  sealed trait Variable extends Node {
    def name: String

    def isLazy: Boolean = !leader.deps.contains(this)

    override val optDeps: Set[Node] = Set.empty
  }

  case class Include(scope: String, flow: String)(
    val args: Map[String, Node], val exports: Map[String, Placeholder]
  ) extends Node {
    override val deps: Set[Node] = Set.empty ++ args.values

    override val optDeps: Set[Node] = Set.empty
  }

  case class Schedule(scope: String, flow: String, trace: Option[Node])(
    val args: Map[String, Node], val waitTime: Duration
  ) extends Node {
    override val deps: Set[Node] = args.values.toSet ++ trace

    override val optDeps: Set[Node] = Set.empty
  }

  sealed trait Node {
    var _leader: Terminator = _

    var _descendants: SortedSet[Node] = SortedSet.empty

    val index: Int = {
      val size = Builder.current.get().nodes.size
      Builder.current.get().nodes += this
      size
    }

    override def equals(obj: Any): Boolean = {
      obj match {
        case that: Node => this eq that
        case _ => false
      }
    }

    override def hashCode(): Int = System.identityHashCode(this)

    def leader: Terminator = _leader

    def descendants: SortedSet[Node] = _descendants

    def deps: Set[Node]

    def optDeps: Set[Node] = Set.empty

    // search nodes accepted by func, along returned candidates
    def search(func: PartialFunction[Node, Iterable[Node]]): SortedSet[Node] = {
      var results = SortedSet.empty[Node]

      def doSearch(node: Node): Unit = {
        if (!results.contains(node) && func.isDefinedAt(node)) {
          results += node
          func(node).foreach(doSearch)
        }
      }

      doSearch(this)
      results
    }
  }

  object Node {
    def search(nodes: Iterable[Node])(func: PartialFunction[Node, Iterable[Node]]): SortedSet[Node] = {
      nodes.map(_.search(func)).fold(SortedSet.empty)(_ ++ _)
    }
  }

  object Builder {
    val current: ThreadLocal[Builder] = new ThreadLocal[Builder]()
  }

  trait Builder {
    var nodes: ArrayBuffer[Node] = new ArrayBuffer()

    def build(): Flow
  }
}

