/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.dsl

import java.util.concurrent.atomic.AtomicInteger

import com.didichuxing.horoscope.core.Flow._
import com.didichuxing.horoscope.core.FlowDslMessage._
import com.didichuxing.horoscope.core.{Compositor, Flow}
import com.didichuxing.horoscope.runtime.expression.{BuiltIn, Expression, ExpressionBuilder}

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.language.implicitConversions
import scala.util.Try

class CompatibleBuilder(flowDef: FlowDef, flowConf: FlowConf)(
  implicit
  buildCompositor: (String, CompositorDef) => Compositor,
  builtin: BuiltIn
) extends Builder {
  import scala.collection.JavaConversions._

  def build(): Flow = {
    val terminator = parseBlock(flowDef.getBody).terminator
    new Flow(flowDef, flowConf)(nodes, terminator, Map.empty, Map.empty)
  }

  val compositors: Map[String, Compositor] = {
    flowDef.getDeclaration.getCompositorList.map(
      desc => (desc.getName, buildCompositor(flowDef.getName, desc))
    ).toMap
  }
  val arguments: mutable.Map[String, Placeholder] = mutable.Map()
  val loads: mutable.Map[String, Load] = mutable.Map()
  val stack: mutable.Stack[Scope] = mutable.Stack()

  def currentScope: Scope = stack.top

  case class Variable(name: String, node: Node, isLazy: Boolean)

  class Scope {
    val parent: Option[Scope] = stack.headOption

    val nodes: mutable.Set[Node] = mutable.Set()
    val variables: mutable.Map[String, Variable] = mutable.Map()
    val choices: mutable.Set[Choice] = mutable.Set()
    var goto: Option[Schedule] = None

    lazy val terminator: Terminator = {
      val variableNodes = variables.values.filter(v => !v.isLazy && !v.node.isInstanceOf[Switch]).map(_.node)
      val deps = choices.toSet ++ variableNodes ++ goto
      val nonDirectDeps = deps.flatMap(n => Node.search(n.deps)({case node: Node => node.deps}))

      val leader = Terminator(deps -- nonDirectDeps)(Set.empty)
      for (node <- nodes) {
        node._leader = leader
      }

      leader
    }

    def resolve(name: String): Node = {
      val result = getVariable(name).map(_.node) orElse {
        if (name.startsWith("$")) {
          Some(load(name))
        } else if (name.startsWith("@")) {
          Some(argument(name))
        } else {
          None
        }
      }
      assert(result.nonEmpty, s"fail to find variable $name")
      result.get
    }

    def load(name: String): Load = {
      loads.getOrElseUpdate(name, {
        val node = Load(name)
        stack.last.nodes += node
        node
      })
    }

    def argument(name: String): Placeholder = {
      arguments.getOrElseUpdate(name, {
        val node = Placeholder(name)
        stack.last.nodes += node
        node
      })
    }

    def getVariable(name: String): Option[Variable] = {
      variables.get(name) orElse {
        parent.flatMap(_.getVariable(name))
      }
    }

    def addBlock(scope: Scope): Unit = {
      nodes += scope.terminator
    }

    def addVariable(name: String, node: Node, isLazy: Boolean): Unit = {
      require(getVariable(name).isEmpty)
      require(!name.startsWith("@"))

      variables.put(name, Variable(name, node, isLazy))
      nodes += node
    }

    def addBranch(branches: Iterable[(String, Node, Scope)]): Unit = {
      val variableCandidates: mutable.Map[String, mutable.Map[String, Variable]] = mutable.Map()

      var lastChoice: Option[Choice] = None
      for ((name, condition, scope) <- branches) {
        val choice = Choice(name, condition)(scope.terminator, lastChoice)
        lastChoice= Some(choice)

        choices += choice
        nodes += choice

        for ((_, variable) <- scope.variables) {
          val candidates = variableCandidates.getOrElseUpdate(variable.name, mutable.Map())
          candidates += choice.name -> variable
        }
      }

      for ((name, candidates) <- variableCandidates) {
        val fallbackCandidate: Option[(String, Node)] = if (name.startsWith("$")) {
          Some("" -> load(name))
        } else {
          None
        }

        val node = Switch(name, candidates.mapValues(_.node).toMap ++ fallbackCandidate, lastChoice.get)
        addVariable(
          name,
          node,
          isLazy = candidates.values.exists(_.isLazy)
        )
      }
    }

    def addGoto(flow: String, args: Map[String, Node], scheduledTime: Duration): Unit = {
      val node = Schedule("", flow, None)(args, scheduledTime)
      goto = Some(node)
      nodes += node
    }
  }
  val expressionBuilder = new ExpressionBuilder(flowDef.getName, builtin)
  implicit def buildExpression(definition: ExprDef): ExpressionHelper =
    new ExpressionHelper(expressionBuilder.build(definition))

  object tempName {
    val count: AtomicInteger = new AtomicInteger(0)

    def apply(): String = s"~${count.getAndIncrement()}"
  }

  implicit class ExpressionHelper(val expr: Expression)  {
    import com.didichuxing.horoscope.runtime.expression.Reference

    def references: Seq[String] = {
      expr match {
        case ref: Reference if ref.name == "_" => Nil
        case ref: Reference => ref.name :: Nil
        case _ => expr.children.flatMap(_.references.map(_.name))
      }
    }

    def evaluate(name: String = ""): Node = {
      val nodeName = if (name.isEmpty) tempName() else name
      val context = references.map(name => (name, currentScope.resolve(name))).toMap
      val node = Evaluate(nodeName, expr, context)()

      currentScope.nodes += node
      node
    }
  }

  def parseBlock(block: Block): Scope = {
    stack.push(new Scope())

    for (statement <- block.getStatementList if currentScope.goto.isEmpty) {
      try {
        import Statement.BodyCase._
        statement.getBodyCase match {
          case ASSIGN_STATEMENT =>
            parseAssignStatement(statement.getAssignStatement)
          case COMPOSITE_STATEMENT =>
            parseCompositeStatement(statement.getCompositeStatement)
          case BRANCH_STATEMENT =>
            parseBranchStatement(statement.getBranchStatement)
          case SCHEDULE_STATEMENT =>
            parseGotoStatement(statement.getScheduleStatement)
          case BODY_NOT_SET =>
        }
      } catch {
        case e: Exception if !e.isInstanceOf[FlowBuildException] =>
          throw new FlowBuildException(s"fail to parse ${statement.getId}", e)
      }
    }
    currentScope.parent.foreach(_.addBlock(currentScope))

    stack.pop()
  }

  def parseAssignStatement(assign: AssignStatement): Unit = {
    currentScope.addVariable(
      assign.getReference,
      assign.getEvaluate.getExpression.evaluate(name = assign.getReference),
      assign.getIsLazy
    )
  }

  def parseCompositeStatement(composite: CompositeStatement): Unit = {
    val expression = buildExpression(composite.getArgument)
    val args: Map[String, Node] = expression.references.map(name => (name, currentScope.resolve(name))).toMap

    currentScope.addVariable(
      composite.getReference,
      Composite(
        composite.getReference,
        composite.getCompositor,
        args
      )(compositors(composite.getCompositor), expression.expr, composite.getIsBatch),
      composite.getIsLazy
    )
  }

  def parseBranchStatement(branch: BranchStatement): Unit = {
    // (name, predicate, action)
    val choices = Iterable.newBuilder[(String, Node, Scope)]
    for (block <- branch.getChoiceList) {
      val scope: Scope = parseBlock(block.getAction)
      for (choice <- block.getConditionList) yield {
        val name = choice.getName
        val predicate = if (choice.hasEvaluate) {
          choice.getEvaluate.getExpression.evaluate(name)
        } else {
          currentScope.resolve(name)
        }
        choices.+=((name, predicate, scope))
      }
    }
    currentScope.addBranch(choices.result())
  }

  def parseGotoStatement(goto: ScheduleStatement): Unit = {
    val args: Map[String, Node] = goto.getArgumentList.map(argument => {
      (argument.getName, argument.getEvaluate.getExpression.evaluate(s"-${argument.getName}->${goto.getFlowName}"))
    }).toMap

    val scheduledTime: Duration = if(goto.hasScheduleTime) {
      Try(Duration(goto.getScheduleTime)).getOrElse(Duration.Zero)
    } else {
      Duration.Zero
    }
    currentScope.addGoto(goto.getFlowName, args, scheduledTime)
  }
}
