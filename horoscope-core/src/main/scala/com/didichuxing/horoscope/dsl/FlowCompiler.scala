/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.dsl

import java.io.StringReader

import com.didichuxing.horoscope.core.FlowDslMessage.FlowDef
import com.didichuxing.horoscope.util.Logging
import com.google.gson.stream.JsonReader
import org.antlr.v4.runtime.tree.{ParseTreeWalker, TerminalNode}
import org.antlr.v4.runtime.{CharStream, CharStreams, CommonTokenStream, ParserRuleContext}
import org.apache.commons.codec.digest.DigestUtils

import scala.collection.mutable

class FlowCompiler extends FlowBaseListener with Logging {
  import com.didichuxing.horoscope.core.FlowDslMessage._

  import scala.collection.JavaConversions._

  private val message = FlowDef.newBuilder()
  private val blocks: mutable.Stack[Block.Builder] = mutable.Stack()
  private val branches: mutable.Stack[BranchStatement.Builder] = mutable.Stack()

  def compile(stream: CharStream): FlowDef = {
    val parser = new FlowParser(new CommonTokenStream(new FlowLexer(stream)))
    parser.removeErrorListeners()
    parser.addErrorListener(SyntaxErrorListener)

    val flow = parser.wholeFlow()
    new ParseTreeWalker().walk(this, flow)

    message.build()
  }

  override def enterFlow(ctx: FlowParser.FlowContext): Unit = {
    message.clear()
    message.setId(DigestUtils.md5Hex(ctx.getText))
    message.setName(ctx.URI().getText)

    for (config <- ctx.flowConfig()) {
      message.putConfig(config.SNAKE().getText, parseString(config.STRING()))
    }

    blocks.clear()
    blocks.push(message.getBodyBuilder)

    branches.clear()
  }

  override def enterCompositor(ctx: FlowParser.CompositorContext): Unit = {
    val code = ctx.CODE().getText
    val lines = code.substring(3, code.length - 3).trim.lines.toSeq

    val builder = message.getDeclarationBuilder.addCompositorBuilder()
    builder.setName(ctx.UCAMEL().getText)
    builder.setFactory(lines.head.trim)
    builder.setContent(lines.tail.mkString("\n"))
  }

  private def newStatement(ctx: ParserRuleContext)(f: Statement.Builder => Unit): Unit = {
    val flow = message.getName
    val position = ctx.start

    val builder = blocks.top.addStatementBuilder()
    builder.setId(s"$flow:${position.getLine}:${position.getCharPositionInLine}")
    f(builder)
  }

  override def enterAssign(ctx: FlowParser.AssignContext): Unit = newStatement(ctx) { statement =>
    val assign = statement.getAssignStatementBuilder
    val naming = ctx.naming()
    assign.setReference(naming.variable().getText)
    assign.setEvaluate(parseEvaluate(ctx.evaluate))
    if (naming.isLazy != null) {
      assign.setIsLazy(true)
    }
    if (naming.isTransient != null) {
      assign.setIsTransient(true)
    }
  }

  override def enterComposite(ctx: FlowParser.CompositeContext): Unit = newStatement(ctx) { statement =>
    val composite = statement.getCompositeStatementBuilder
    composite.setCompositor(ctx.UCAMEL().getText)
    val naming = ctx.naming()
    if (naming.variable() != null) {
      composite.setReference(naming.variable().getText)
      if (naming.isLazy != null) {
        composite.setIsLazy(true)
      }
      if (naming.isTransient != null) {
        composite.setIsTransient(true)
      }
    } else {
      composite.setReference(
        "[A-Z][a-z0-9]*".r.findAllIn(composite.getCompositor).map(_.toLowerCase()).mkString("_")
      )
    }

    val arguments = composite.getArgumentBuilder.getDictConstructorBuilder
    for (context <- ctx.compositeArgumentList().compositeArgument()) {
      val name = context.name().getText
      val argument = arguments.addElementBuilder()
      argument.setName(name)
      if (context.expression() != null) {
        argument.setChild(parseExpression(context.expression()))
      } else {
        argument.getChildBuilder.getReferenceBuilder.setName(name)
      }
    }
  }

  override def enterBatchComposite(ctx: FlowParser.BatchCompositeContext): Unit = newStatement(ctx) { statement =>
    val composite = statement.getCompositeStatementBuilder
    composite.setCompositor(ctx.UCAMEL().getText)
    composite.setIsBatch(true)
    val naming = ctx.naming()
    if (naming.variable() != null) {
      composite.setReference(naming.variable().getText)
      if (naming.isLazy != null) {
        composite.setIsLazy(true)
      }
      if (naming.isTransient != null) {
        composite.setIsTransient(true)
      }
    } else {
      composite.setReference(
        "[A-Z][a-z0-9]*".r.findAllIn(composite.getCompositor).map(_.toLowerCase()).mkString("_")
      )
    }

    if (ctx.compositeArgumentList() == null) {
      composite.setArgument(parseExpression(ctx.expression()))
    } else {
      val transform = composite.getArgumentBuilder.getTransformBuilder
      transform.setFrom(parseExpression(ctx.expression()))

      val arguments = transform.getExpressionBuilder.getDictConstructorBuilder
      for (context <- ctx.compositeArgumentList().compositeArgument()) {
        val name = context.name().getText
        val argument = arguments.addElementBuilder()
        argument.setName(name)
        if (context.expression() != null) {
          argument.setChild(parseExpression(context.expression()))
        } else {
          argument.getChildBuilder.getReferenceBuilder.setName(name)
        }
      }
    }
  }

  override def enterSchedule(ctx: FlowParser.ScheduleContext): Unit = newStatement(ctx) { statement =>
    val schedule = statement.getScheduleStatementBuilder
    schedule.setScope(ctx.SNAKE().getText)
    schedule.setFlowName(ctx.URI().getText)
    for (argument <- ctx.flowArgumentList().namedExpression()) {
      schedule.addArgument(parseNamedExpressions(argument))
    }

    if (ctx.STRING() != null) {
      schedule.setScheduleTime(parseString(ctx.STRING()))
    }

    if (ctx.trace != null) {
      schedule.setTrace(parseEvaluate(ctx.trace))
    }
  }

  override def enterInclude(ctx: FlowParser.IncludeContext): Unit = newStatement(ctx) { statement =>
    val include = statement.getIncludeStatementBuilder
    include.setScope(ctx.SNAKE().getText)
    include.setFlowName(ctx.URI().getText)
    for (argument <- ctx.flowArgumentList().namedExpression()) {
      include.addArgument(parseNamedExpressions(argument))
    }
  }

  override def enterBranch(ctx: FlowParser.BranchContext): Unit = newStatement(ctx) { statement =>
    branches.push(statement.getBranchStatementBuilder)
  }

  override def exitBranch(ctx: FlowParser.BranchContext): Unit = {
    branches.pop()
  }

  override def enterChoice(ctx: FlowParser.ChoiceContext): Unit = {
    val choice = branches.top.addChoiceBuilder()
    for (condition <- ctx.namedExpression()) {
      choice.addCondition(parseNamedExpressions(condition))
    }
    blocks.push(choice.getActionBuilder)
  }

  override def exitChoice(ctx: FlowParser.ChoiceContext): Unit = {
    blocks.pop()
  }

  private def parseEvaluate(ctx: FlowParser.EvaluateContext): EvaluateDef.Builder = {
    val builder = EvaluateDef.newBuilder()
    builder.setExpression(parseExpression(ctx.expr))
    if(ctx.failover != null) {
      builder.setFailover(parseExpression(ctx.failover))
    }
    builder
  }

  private def parseNamedExpressions(ctx: FlowParser.NamedExpressionContext): NamedExpression.Builder = {
    val builder = NamedExpression.newBuilder()
    builder.setName(ctx.name().getText)
    if (ctx.evaluate() != null) {
      builder.setEvaluate(parseEvaluate(ctx.evaluate()))
    }
    builder
  }

  private val parser = new ExpressionParser()
  private def parseExpression(ctx: ParserRuleContext): ExprDef.Builder = {
    parser.visit(ctx)
  }

  private def parseString(node: TerminalNode): String = {
    val reader = new JsonReader(new StringReader(node.getText))
    reader.nextString()
  }
}

object FlowCompiler {
  def compile(text: String): FlowDef = {
    val compiler = new FlowCompiler()
    compiler.compile(CharStreams.fromString(text))
  }
}
