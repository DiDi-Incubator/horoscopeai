/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.dsl

import com.didichuxing.horoscope.core.FlowConf
import com.didichuxing.horoscope.core.FlowConf._
import com.didichuxing.horoscope.core.FlowDslMessage.{CompositorDef, FlowDef}
import com.didichuxing.horoscope.core.{Compositor, Flow}
import com.didichuxing.horoscope.runtime.expression.{BuiltIn, Expression, ExpressionBuilder}

import scala.collection.JavaConversions._
import scala.collection.immutable.{ListMap, SortedSet}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.language.implicitConversions
import scala.util.Try
import com.didichuxing.horoscope.util.FlowConfParser._
import com.didichuxing.horoscope.util.Logging

class FlowBuilder(flowDef: FlowDef, flowConf: FlowConf)(
  implicit
  buildCompositor: (String, CompositorDef) => Compositor,
  builtin: BuiltIn
) extends Flow.Builder with Logging {

  import Flow._
  import com.didichuxing.horoscope.core.FlowDslMessage
  import com.didichuxing.horoscope.runtime.expression.Reference

  private val compositors: Map[String, Compositor] = {
    flowDef.getDeclaration.getCompositorList.map(
      desc => (desc.getName, Try(buildCompositor(flowDef.getName, desc)).recover({
        case e: Throwable => Compositor.failed(e)
      }).get)
    ).toMap
  }
  private val expressionBuilder = new ExpressionBuilder(flowDef.getName, builtin)

  private val usedNames: mutable.Set[String] = mutable.Set()

  private val placeholders: mutable.Map[String, Placeholder] = mutable.Map.empty

  def build(): Flow = {
    val main: BlockBuilder = new BlockBuilder(flowDef.getBody, None).withFlowConf(flowConf)
    val terminator = main.result
    new Flow(flowDef, flowConf)(
      nodes = nodes,
      terminator = terminator,
      arguments = placeholders.filter(_._2.name.startsWith("@")).toMap,
      variables = main.variables.mapValues(_.variable.result)
    )
  }

  trait GenericNodeBuilder[+N <: Node] {
    def result: N
  }

  type NodeBuilder = GenericNodeBuilder[Node]

  object NodeBuilder {
    def apply[N <: Node](factory: => N): GenericNodeBuilder[N] = {
      new GenericNodeBuilder[N] {
        lazy val result: N = factory
      }
    }
  }

  sealed trait Symbol {
    def name: String

    def builders: Seq[NodeBuilder]

    def isLazy: Boolean

    def isTransient: Boolean
  }

  case class VariableSymbol(
    name: String, variable: GenericNodeBuilder[Variable], isLazy: Boolean, isTransient: Boolean
  ) extends Symbol {
    override def builders: Seq[NodeBuilder] = variable :: Nil
  }

  sealed trait ProcedureSymbol extends Symbol {
    val scope: String

    def name: String = scope

    def isLazy: Boolean = false

    def isTransient: Boolean = false

    def export(name: String): NodeBuilder

    def placeholder(name: String): GenericNodeBuilder[Placeholder] = {
      val identifier = s"$scope->$name"
      val placeholder = placeholders.getOrElseUpdate(identifier, Placeholder(identifier))
      NodeBuilder(placeholder)
    }
  }

  case class ExportProxy(scope: String, children: Seq[ProcedureSymbol]) extends ProcedureSymbol {
    override def builders: Seq[NodeBuilder] = Nil

    override def export(name: String): NodeBuilder = {
      children.foreach(_.export(name))
      placeholder(name)
    }
  }

  case class IncludeBuilder(scope: String, flow: String)(
    args: Map[String, NodeBuilder], token: Option[NodeBuilder]
  ) extends GenericNodeBuilder[Include] with ProcedureSymbol {
    lazy val result: Include = Include(scope, flow)(
      args.mapValues(_.result), exports.mapValues(_.result), token.map(_.result)
    )

    override def builders: Seq[NodeBuilder] = this :: Nil

    var exports: Map[String, GenericNodeBuilder[Placeholder]] = Map.empty

    def export(name: String): NodeBuilder = {
      if (!exports.contains(name)) {
        exports += name -> placeholder(name)
      }
      exports(name)
    }
  }

  case class CallbackBuilder(scope: String, flow: String, token: NodeBuilder, definition: CallbackConf)(
    args: Map[String, NodeBuilder], timeout: Duration, timeoutFlow: String
  ) extends GenericNodeBuilder[Callback] with ProcedureSymbol {
    lazy val result: Callback = {
      Callback(scope, flow, token.result, definition)(
        args.mapValues(_.result), timeout, timeoutFlow)
    }

    override def export(name: String): NodeBuilder = {
      panic(s"should not export variable $name from callback $scope")
    }

    override def builders: Seq[NodeBuilder] = this :: Nil
  }

  case class SubscribeBuilder(scope: String, flow: String, definition: SubscribeConf)(
    args: Map[String, NodeBuilder], condition: Option[NodeBuilder], target: Option[NodeBuilder], traffic: Bucket
  ) extends GenericNodeBuilder[Subscribe] with ProcedureSymbol {
    lazy val result: Subscribe = Subscribe(scope, flow, definition)(
      args.mapValues(_.result), condition.map(_.result), target.map(_.result), traffic)

    override def builders: Seq[NodeBuilder] = this :: Nil

    def export(name: String): NodeBuilder = {
      panic(s"should not export variable $name from subscribe procedure $scope")
    }
  }

  case class ScheduleBuilder(scope: String, flow: String)(
    args: Map[String, NodeBuilder], trace: Option[NodeBuilder], waitTime: Duration, token: Option[NodeBuilder] = None
  ) extends GenericNodeBuilder[Schedule] with ProcedureSymbol {
    lazy val result: Schedule = Schedule(scope, flow, trace.map(_.result))(
      args.mapValues(_.result), waitTime, token.map(_.result))

    override def builders: Seq[NodeBuilder] = this :: Nil

    def export(name: String): NodeBuilder = {
      panic(s"should not export variable $name from scheduled procedure $scope")
    }
  }

  class BranchBuilder(
    blocks: Seq[BlockBuilder], choices: ListMap[String, (NodeBuilder, BlockBuilder)]
  ) extends GenericNodeBuilder[Choice] {
    val isLazy: Boolean = blocks.forall(_.deps.isEmpty)

    val symbols: Map[String, Symbol] = {
      val names = blocks.flatMap(_.symbols.keySet)
      names.map(name => {
        val candidates: Map[String, Symbol] = choices collect {
          case (choice, (_, block)) if block.symbols.contains(name) =>
            choice -> block.symbols(name)
        }

        val isVariable = candidates.values.forall(_.isInstanceOf[VariableSymbol])
        val isProcedure = candidates.values.forall(_.isInstanceOf[ProcedureSymbol])
        require(
          isVariable || isProcedure,
          s"$name is both variable and procedure in different branches"
        )

        val proxy: Symbol = if (isVariable) {
          val switchCandidates = candidates.mapValues(_.asInstanceOf[VariableSymbol].variable.result)
          VariableSymbol(name, NodeBuilder(Switch(name, switchCandidates, result)), isLazy = true, isTransient = false)
        } else {
          ExportProxy(name, candidates.values.map(_.asInstanceOf[ProcedureSymbol]).toSeq)
        }

        name -> proxy
      }).toMap
    }

    lazy val result: Choice = {
      var last: Option[Choice] = None
      for ((name, (condition, block)) <- choices) {
        val choice = Choice(name, condition.result)(block.result, last)
        last = Some(choice)
      }

      last.get
    }
  }

  class BlockBuilder(
    block: FlowDslMessage.Block, parent: Option[BlockBuilder]
  ) extends GenericNodeBuilder[Terminator] {
    val children: ListBuffer[NodeBuilder] = ListBuffer.empty

    val loads: ListBuffer[NodeBuilder] = ListBuffer.empty
    val updates: ListBuffer[NodeBuilder] = ListBuffer.empty
    val branches: ListBuffer[BranchBuilder] = ListBuffer.empty
    var symbols: Map[String, Symbol] = Map.empty

    def variables: Map[String, VariableSymbol] = {
      symbols collect { case (key, symbol: VariableSymbol) => (key, symbol) }
    }

    def procedures: Map[String, ProcedureSymbol] = {
      symbols collect { case (key, symbol: ProcedureSymbol) => (key, symbol) }
    }

    def deps: Iterable[NodeBuilder] = {
      loads ++ updates ++ branches.filter(!_.isLazy) ++
        symbols.values.filterNot(s => s.isLazy || s.isTransient).flatMap(_.builders)
    }

    def optDeps: Iterable[NodeBuilder] = {
      branches.filter(_.isLazy) ++ symbols.values.filter(s => s.isLazy || s.isTransient).flatMap(_.builders)
    }


    lazy val result: Terminator = {
      import Flow.nodeOrdering

      val totalNodes = children.view.map(_.result).toArray
      val terminator = Terminator(SortedSet.empty ++ deps.map(_.result))(SortedSet.empty ++ optDeps.map(_.result))

      for (node <- totalNodes) {
        node._leader = terminator
      }

      for (branch <- branches; choice <- branch.result.branch) {
        choice._leader = terminator
        choice.action._leader = terminator
      }

      terminator
    }

    def addVariable(name: String, build: String => GenericNodeBuilder[Variable],
      isLazy: Boolean, isTransient: Boolean
    ): NodeBuilder = {
      val variableName = if (name.startsWith("$")) name.substring(1) else name
      require(!symbols.contains(variableName), s"symbol $variableName already exists")

      val builder = build(variableName)
      children += builder
      symbols += name -> VariableSymbol(variableName, builder, isLazy, isTransient)

      if (name.startsWith("$")) {
        val update = NodeBuilder(Update(name, builder.result, isTransient))
        children += update
        updates += update
      }

      builder
    }

    def resolve(name: String): NodeBuilder = {
      if (name.startsWith("$")) {
        val load = NodeBuilder(Load(name))
        children += load
        loads += load
        load
      } else if (name.startsWith("@")) {
        val argument = placeholders.getOrElseUpdate(name.substring(1), Placeholder(name))
        NodeBuilder(argument)
      } else {
        {
          variables.get(name).map(_.variable)
        } orElse {
          parent.map(_.resolve(name))
        } getOrElse {
          panic(s"can not find variable $name")
        }
      }
    }

    def export(scope: String, name: String): NodeBuilder = {
      {
        procedures.get(scope).map(_.export(name))
      } orElse {
        parent.map(_.export(scope, name))
      } getOrElse {
        panic(s"can not find procedure $scope")
      }
    }

    implicit class ExpressionHelper(val expr: Expression) {
      def references: Seq[Reference] = {
        expr match {
          case ref: Reference if ref.name == "_" || ref.name == "#" => Nil
          case ref: Reference => ref :: Nil
          case _ => expr.children.flatMap(_.references)
        }
      }

      def args: Map[String, NodeBuilder] = {
        references.map(r => r.identifier -> r.scope.map(export(_, r.name)).getOrElse(resolve(r.name))).toMap
      }
    }

    implicit class EvaluateHelper(val message: FlowDslMessage.EvaluateDef) {
      def evaluate(finalName: String, stageName: String = "",
        isTransient: Boolean = false): GenericNodeBuilder[Evaluate] = {
        val name = if (stageName.isEmpty) finalName else makeTempName(finalName, stageName)

        val expr: Expression = expressionBuilder.build(message.getExpression)
        val args: Map[String, NodeBuilder] = expr.args

        val failover: Option[Expression] = if (message.hasFailover) {
          Some(expressionBuilder.build(message.getFailover))
        } else {
          None
        }
        val failoverArgs: Map[String, NodeBuilder] = failover.map(_.args).getOrElse(Map.empty)

        val builder = NodeBuilder {
          Evaluate(name, expr, args.mapValues(_.result))(failover, failoverArgs.mapValues(_.result), isTransient)
        }
        children += builder

        builder
      }
    }

    implicit class NamedExpressionHelper(val message: FlowDslMessage.NamedExpression) {
      def evaluateFor(finalName: String): NodeBuilder = {
        if (message.hasEvaluate) {
          message.getEvaluate.evaluate(finalName, message.getName)
        } else {
          resolve(message.getName)
        }
      }
    }

    implicit class TupleExpressionHelper(val namedExpression: (String, String)) {
      def evaluateFor(finalName: String): NodeBuilder = {
        val (originalName, expression) = namedExpression
        val name = if (originalName.isEmpty) finalName else makeTempName(finalName, originalName)
        val expr = Expression(flowDef.getName, expression)
        val args = expr.args
        val builder = NodeBuilder {
          Evaluate(name, expr, args.mapValues(_.result))()
        }

        children += builder
        builder
      }
    }

    implicit class ArgumentMapHelper(val args: Map[String, String]) {
      def evaluateFor(finalName: String): Map[String, NodeBuilder] = {
        args.map(arg => (arg._1, arg.evaluateFor(finalName)))
      }
    }

    implicit class ArgumentListHelper(val args: java.util.List[FlowDslMessage.NamedExpression]) {
      def evaluateFor(finalName: String): Map[String, NodeBuilder] = {
        args.map(arg => (arg.getName, arg.evaluateFor(finalName))).toMap
      }
    }

    block.getStatementList.foreach(parseStatement)

    def parseStatement(statement: FlowDslMessage.Statement): Unit = {
      import FlowDslMessage.Statement.BodyCase._
      statement.getBodyCase match {
        case ASSIGN_STATEMENT =>
          parseAssignStatement(statement.getAssignStatement)

        case COMPOSITE_STATEMENT =>
          parseCompositeStatement(statement.getCompositeStatement)

        case INCLUDE_STATEMENT =>
          parseIncludeStatement(statement.getIncludeStatement)

        case SCHEDULE_STATEMENT =>
          parseScheduleStatement(statement.getScheduleStatement)

        case BRANCH_STATEMENT =>
          parseBranchStatement(statement.getBranchStatement)
      }
    }

    def withFlowConf(flowConf: FlowConf): this.type = {
      flowConf.parseSubscribeConf.foreach(parseSubscribe)
      flowConf.parseCallbackConf.foreach(parseCallback)

      this
    }

    def parseSubscribe(subscribe: SubscribeConf): Unit = {
      try {
        val scope = subscribe.name
        require(!symbols.contains(scope), s"symbol $scope already exists")

        val args = subscribe.args.evaluateFor(scope)
        val condition = subscribe.condition.map("condition" -> _).map(_.evaluateFor(scope))
        val target = subscribe.target.map("target" -> _).map(_.evaluateFor(scope))
        val builder = SubscribeBuilder(scope, subscribe.subscriber, definition = subscribe)(
          args, condition, target, subscribe.traffic)

        children += builder
        symbols += scope -> builder
      } catch {
        case cause: Throwable =>
          error(s"parse subscribe config failed, name: ${subscribe.name}, cause: ${cause.getMessage}")
      }
    }

    def parseCallback(callback: CallbackConf): Unit = {
      try {
        val scope = callback.name
        require(!symbols.contains(scope), s"symbol $scope already exists")

        val args = callback.args.evaluateFor(scope)
        val token = ("token" -> callback.token).evaluateFor(scope)
        val builder = CallbackBuilder(scope, callback.callbackFlow, token, callback)(
          args, Duration(callback.timeout), callback.timeoutFlow)
        children += builder
        symbols += scope -> builder
      } catch {
        case cause: Throwable =>
          error(s"parse callback config failed, name: ${callback.name}, cause: ${cause.getMessage}")
      }
    }

    def parseAssignStatement(message: FlowDslMessage.AssignStatement): Unit = {
      addVariable(message.getReference,
        name => message.getEvaluate.evaluate(name, isTransient = message.getIsTransient),
        message.getIsLazy,
        message.getIsTransient
      )
    }

    def parseCompositeStatement(message: FlowDslMessage.CompositeStatement): Unit = {
      addVariable(
        message.getReference,
        name => {
          val expression = expressionBuilder.build(message.getArgument)
          val args = expression.args.mapValues(_.result)
          NodeBuilder {
            Composite(name, message.getCompositor, args)(
              compositors(message.getCompositor), expression, message.getIsBatch, message.getIsTransient
            )
          }
        },
        message.getIsLazy,
        message.getIsTransient
      )
    }

    def parseIncludeStatement(message: FlowDslMessage.IncludeStatement): Unit = {
      val scope = message.getScope
      require(!symbols.contains(scope), s"symbol $scope already exists")

      val token = if (message.hasToken) {
        Some(message.getToken.evaluate(scope))
      } else {
        None
      }
      val builder = IncludeBuilder(scope, message.getFlowName
      )(message.getArgumentList.evaluateFor(scope), token)
      children += builder
      symbols += scope -> builder
    }

    def parseScheduleStatement(message: FlowDslMessage.ScheduleStatement): Unit = {
      val scope = message.getScope
      require(!symbols.contains(scope), s"symbol $scope already exists")

      val args = message.getArgumentList.evaluateFor(scope)
      val trace = if (message.hasTrace) {
        Some(message.getTrace.evaluate(scope, "#"))
      } else {
        None
      }
      val duration = if (message.hasScheduleTime) {
        Duration(message.getScheduleTime)
      } else {
        Duration.Zero
      }

      val token = if (message.hasToken) {
        Some(message.getToken.evaluate(scope, "#"))
      } else {
        None
      }

      val builder = ScheduleBuilder(scope, message.getFlowName)(args, trace, duration, token)
      children += builder
      symbols += scope -> builder
    }

    def parseBranchStatement(message: FlowDslMessage.BranchStatement): Unit = {
      val blocks = Seq.newBuilder[BlockBuilder]
      val choices = ListMap.newBuilder[String, (NodeBuilder, BlockBuilder)]
      for (choice <- message.getChoiceList) {
        val block = new BlockBuilder(choice.getAction, Some(this))
        blocks += block

        for (condition <- choice.getConditionList) {
          val node = if (condition.hasEvaluate) {
            addVariable(condition.getName, condition.getEvaluate.evaluate(_), isLazy = true, isTransient = false)
          } else {
            resolve(condition.getName)
          }
          choices += condition.getName -> (node, block)
        }
      }

      val branch = new BranchBuilder(blocks.result(), choices.result())
      children += branch
      branches += branch

      val conflictSymbols = symbols.keySet.intersect(branch.symbols.keySet)
      require(conflictSymbols.isEmpty, conflictSymbols.mkString("symbol ", ",", "already exists"))
      symbols ++= branch.symbols
    }
  }

  def panic(message: String): Nothing = {
    throw new SemanticException(message)
  }

  def require(requirement: Boolean, message: => String): Unit = {
    if (!requirement) {
      throw new SemanticException(message)
    }
  }

  def makeTempName(finalName: String, stageName: String, suffix: String = ""): String = {
    val proposal = s"-$stageName$suffix->$finalName"
    if (usedNames.contains(proposal)) {
      makeTempName(finalName, stageName, suffix + "~")
    } else {
      usedNames += proposal
      proposal
    }
  }
}
