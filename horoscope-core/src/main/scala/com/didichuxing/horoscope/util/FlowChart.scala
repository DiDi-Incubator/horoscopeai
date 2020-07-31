/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.util

import java.nio.file.Files
import java.util.concurrent.atomic.AtomicLong

import com.didichuxing.horoscope.core.Flow

import scala.collection.SortedSet
import scala.collection.immutable.SortedMap
import scala.collection.mutable.ListBuffer

class FlowChart(flow: Flow) extends Logging {
  import Flow._

  var seq: AtomicLong = new AtomicLong(0)

  implicit def expressionOrdering: Ordering[Evaluate] = Ordering.by(_.index)

  trait Symbol {
    val id: String = s"s${seq.incrementAndGet()}"

    def attributes: Map[String, String] = Map.empty

    def toDot: Iterator[String] = s"""$id ${
      attributes.map({ case (k, v) => s"""$k=$v"""}).mkString("[", " ", "];")
    }""".lines

    def quote(text: => String): String = {
      text.replace("\"", "\\\"").lines.mkString("\"", "\\n", "\"")
    }
  }

  implicit def symbolOrdering: Ordering[Symbol] = Ordering.by(_.id)

  class ExpressionSymbol(val expressions: Seq[Evaluate]) extends Symbol {
    override def attributes: Map[String, String] = Map(
      "shape" -> "parallelogram",
      // "label" -> quote(expressions.map(e => s"${e.name} = ${e.expression}").mkString("\n"))
      "label" -> quote(expressions.map(e => s"${e.name}").mkString("\n"))
    )
  }

  class ActionSymbol(val node: Composite) extends Symbol {
    override def attributes: Map[String, String] = Map(
      "shape" -> "rect",
      "label" -> quote(node.compositor)
    )
  }

  class DecisionSymbol(val node: Choice) extends Symbol {
    override def attributes: Map[String, String] = Map(
      "shape" -> "diamond",
      "label" -> quote {
        s"${node.asInstanceOf[Choice].name.replace("_", "\n")} \n?"
      }
    )
  }

  class ConnectorSymbol(val node: Node) extends Symbol {
    override def attributes: Map[String, String] = Map(
      "shape" -> "point"
    )
  }

  // symbols one by one, from left to right
  class Stream {
    var symbols: SortedSet[Symbol] = SortedSet.empty

    var branches: Map[Symbol, Branch] = Map.empty

    def toDot: Iterator[String] = {
      val main = if (symbols.isEmpty) {
        Iterator.empty
      } else {
        Iterator(symbols.map(_.id).mkString("", " -> ", ";"))
      }
      main ++ branches.valuesIterator.flatMap(_.toDot)
    }
  }

  // parallel choices from bottom to top
  class Branch {
    implicit def decisionOrdering: Ordering[DecisionSymbol] = Ordering.by(_.node.index)

    case class Context(preparation: Seq[Symbol], stream: Stream)

    var decisions: SortedMap[DecisionSymbol, Context] = SortedMap.empty

    def entry: Symbol = {
      val (decision, context) = decisions.head
      (context.preparation :+ decision).head
    }

    def toDot: Iterator[String] = {
      val lines: ListBuffer[String] = ListBuffer()

      var lastDecision: Option[DecisionSymbol] = None
      for ((decision, Context(preparation, stream)) <- decisions) {
        lines += (preparation :+ decision).map(_.id).mkString("", " -> ", ";")
        lines ++= stream.symbols.headOption.map(action => s"${decision.id} -> ${action.id} [label=yes];")
        lines ++= lastDecision.map(last => s"${last.id} -> ${preparation.head.id} [label=no];")

        lines ++= stream.toDot

        lastDecision = Some(decision)
      }

      val ranks = decisions.flatMap({case (decision, context) => context.preparation :+ decision})
      lines ++= s"{ rank = same; ${ranks.map(_.id).mkString("", "; ", ";")}}".lines

      lines.iterator
    }
  }

  val symbols: ListBuffer[Symbol] = ListBuffer()

  val relations: ListBuffer[(Symbol, Symbol)] = ListBuffer()

  val mainStream: Stream = new SymbolContext().buildStream(flow.terminator)

  def toDot: String = {
    val head = "digraph G {"
    val body: Seq[String] = Seq(
      "rankdir=LR;"
    ) ++ symbols.flatMap(_.toDot) ++ mainStream.toDot /* ++ relations.map({
      case (from, to) => s"${from.id} -> ${to.id} [style=dotted];"
    }) */
    val last = "}"

    (head +: body.map("  " + _) :+ last).mkString("\n")
  }

  def show(): Unit = {
    import scala.sys.process._

    val tmp = Files.createTempFile("flow", ".dot")
    logging.warn(s"write dot file to $tmp")
    Files.write(tmp, toDot.getBytes)

    if (System.getProperty("os.name") == "Mac OS X") {
      Seq("killall", "Graphviz").!
      Seq("open", "-a", "Graphviz", "--args", tmp.toString).!
    } else {
      Seq("gvedit", tmp.toString).!
    }
  }

  class SymbolContext(val foreignReferences: Map[Node, Symbol] = Map.empty) {
    var localReferences: Map[Node, Symbol] = Map.empty

    def references: Map[Node, Symbol] = foreignReferences ++ localReferences

    def newContext(): SymbolContext = new SymbolContext(references)

    def buildExpression(evaluates: Iterable[Evaluate]): Option[ExpressionSymbol] = {
      val expressions = evaluates.filterNot(references.contains)
      if (expressions.nonEmpty) {
        val symbol = new ExpressionSymbol(expressions.toSeq)

        symbols += symbol
        expressions.foreach(localReferences += _ -> symbol)

        Some(symbol)
      } else {
        None
      }
    }

    def buildAction(composite: Composite): Seq[Symbol] = {
      val prerequisites = Node.search(composite.deps) {
        case evaluate: Evaluate if !references.contains(evaluate) => evaluate.deps
      }

      val (arguments, expressions) = prerequisites.toSeq.collect({
        case evaluate: Evaluate => evaluate
      }).partition(_.name.startsWith("-"))

      val preparation = buildExpression(expressions).toSeq
      val symbol = new ActionSymbol(composite)

      symbols += symbol
      localReferences ++= (arguments :+ composite).map(_ -> symbol)

      val foreigners = Node.search(composite.deps) {
        case evaluate: Evaluate if foreignReferences.contains(evaluate) => Nil
        case evaluate: Evaluate => evaluate.deps
      }
      for (node <- foreigners; foreignSymbol <- foreignReferences.get(node)) {
        relations += foreignSymbol -> symbol
      }

      preparation :+ symbol
    }

    // scalastyle:off
    def buildStream(terminator: Terminator): Stream = {
      val stream = new Stream

      val candidates = Node.search(terminator.deps) {
        case evaluate: Evaluate if !references.contains(evaluate) => evaluate.deps
        case composite: Composite if composite.isLazy && !references.contains(composite) => composite.deps
        case choice: Choice if choice.leader == terminator => Nil
        case node: Node if node.leader == terminator => node.deps
      }

      candidates foreach {
        case choice: Choice =>
          val branch = newContext().buildBranch(choice.entry)
          stream.symbols += branch.entry
          stream.branches += branch.entry -> branch

        case composite: Composite =>
          stream.symbols ++= buildAction(composite)

        case _ =>
      }

      stream
    }

    def buildBranch(entry: Choice): Branch = {
      val branch = new Branch()

      val choices = entry.search({
        case choice: Choice if choice.entry == entry => choice.descendants
      }).collect({ case choice: Choice => choice })

      for (choice <- choices) {
        val requirements = choice.condition search {
          case node: Node if references.contains(node) => Nil
          case evaluate: Evaluate => evaluate.deps
          case composite: Composite if composite.isLazy => composite.deps
        }

        val preparation = requirements.toSeq.flatMap({
          case composite: Composite if !references.contains(composite) => buildAction(composite)
          case _ => Nil
        }) ++ buildExpression(
          requirements.collect({ case evaluate: Evaluate => evaluate})
        )

        val decision = new DecisionSymbol(choice)
        symbols += decision

        val context = branch.Context(preparation, newContext().buildStream(choice.action))
        branch.decisions += decision -> context
      }

      branch
    }
  }
}
