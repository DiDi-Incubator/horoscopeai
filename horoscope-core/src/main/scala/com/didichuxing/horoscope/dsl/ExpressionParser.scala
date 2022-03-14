package com.didichuxing.horoscope.dsl

import java.io.StringReader

import com.didichuxing.horoscope.core.FlowDslMessage.{EvaluateDef, ExprDef, Operator}
import com.google.gson.stream.JsonReader
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.TerminalNode
import org.antlr.v4.runtime.{CharStream, CharStreams, CommonTokenStream, ParserRuleContext}

// scalastyle:off
class ExpressionParser extends FlowBaseVisitor[ExprDef.Builder] {
  import Operator._

  import scala.collection.JavaConversions._

  def parse(text: String): ExprDef = parse(CharStreams.fromString(text))

  def parse(stream: CharStream): ExprDef = {
    val parser = new FlowParser(new CommonTokenStream(new FlowLexer(stream)))
    parser.removeErrorListeners()
    parser.addErrorListener(SyntaxErrorListener)

    visit(parser.wholeExpression().expression()).build()
  }

  override def visitExpression(ctx: FlowParser.ExpressionContext): ExprDef.Builder = {
    if (ctx.value() != null) {
      visit(ctx.value())
    } else {
      visit(ctx.predicate())
    }
  }

  override def visitSubValue(ctx: FlowParser.SubValueContext): ExprDef.Builder = {
    visit(ctx.value())
  }

  override def visitSubPredicate(ctx: FlowParser.SubPredicateContext): ExprDef.Builder = {
    visit(ctx.predicate())
  }

  override def visitPlaceholder(ctx: FlowParser.PlaceholderContext): ExprDef.Builder = newBuilder(ctx) { builder =>
    builder.getReferenceBuilder.setName(ctx.getText)
  }

  override def visitMember(ctx: FlowParser.MemberContext): ExprDef.Builder = newBuilder(ctx) { builder =>
    builder.getReferenceBuilder.setName(ctx.getText)
  }

  override def visitReference(ctx: FlowParser.ReferenceContext): ExprDef.Builder = newBuilder(ctx) { builder =>
    builder.getReferenceBuilder.setName(ctx.variable.getText)
    if  (ctx.scope != null) {
      builder.getReferenceBuilder.setScope(ctx.scope.getText)
    }
  }

  override def visitLiteral(ctx: FlowParser.LiteralContext): ExprDef.Builder = newBuilder(ctx) { builder =>
    val primitive = ctx.primitive()
    val literal = builder.getLiteralBuilder

    if (primitive.STRING() != null) {
      literal.setStringValue(visitString(primitive.STRING()))
    }

    if (primitive.NUMBER() != null) {
      literal.setNumberValue(primitive.NUMBER().getText)
    }

    if (primitive.BOOLEAN() != null) {
      literal.setBooleanValue(primitive.BOOLEAN().getText.toBoolean)
    }
  }

  override def visitCall(ctx: FlowParser.CallContext): ExprDef.Builder = newBuilder(ctx) { builder =>
    val call = builder.getCallBuilder
    call.setFunction(ctx.function().getText)
    for (argument <- ctx.expression()) {
      call.addArgument(visit(argument))
    }
  }

  override def visitApply(ctx: FlowParser.ApplyContext): ExprDef.Builder = newBuilder(ctx) { builder =>
    val apply = builder.getApplyBuilder
    apply.setMethod(ctx.function().getText)
    apply.setFrom(visit(ctx.from))
    for (argument <- ctx.expression()) {
      apply.addArgument(visit(argument))
    }
  }

  override def visitNewObject(ctx: FlowParser.NewObjectContext): ExprDef.Builder = newBuilder(ctx) { builder =>
    val objectConstructor = builder.getDictConstructorBuilder
    for (pair <- ctx.obj().pair()) {
      objectConstructor.addElementBuilder()
        .setName(visitString(pair.STRING()))
        .setChild(visit(pair.expression))
    }
  }

  override def visitNewArray(ctx: FlowParser.NewArrayContext): ExprDef.Builder = newBuilder(ctx) { builder =>
    val arrayConstructor = builder.getListConstructorBuilder
    for (element <- ctx.array().expression()) {
      arrayConstructor.addElement(visit(element))
    }
  }

  override def visitVisit(ctx: FlowParser.VisitContext): ExprDef.Builder = newBuilder(ctx) { builder =>
    builder.getVisitBuilder
      .setName(ctx.property().getText)
      .setFrom(visit(ctx.from))
  }

  override def visitVisitAll(ctx: FlowParser.VisitAllContext): ExprDef.Builder = newBuilder(ctx) { builder =>
    builder.getVisitBuilder
      .setFrom(visit(ctx.from))
  }

  override def visitAt(ctx: FlowParser.AtContext): ExprDef.Builder = newBuilder(ctx) { builder =>
    builder.getAtBuilder
      .setIndex(visit(ctx.index))
      .setFrom(visit(ctx.from))
  }

  override def visitSearch(ctx: FlowParser.SearchContext): ExprDef.Builder = newBuilder(ctx) { builder =>
    builder.getSearchBuilder
      .setName(ctx.property().getText)
      .setFrom(visit(ctx.from))
  }

  override def visitSearchAll(ctx: FlowParser.SearchAllContext): ExprDef.Builder = newBuilder(ctx) { builder =>
    builder.getSearchBuilder.setFrom(visit(ctx.from))
  }

  override def visitSelect(ctx: FlowParser.SelectContext): ExprDef.Builder = newBuilder(ctx) { builder =>
    val selection = builder.getSelectBuilder
    selection.setFrom(visit(ctx.from))
    for (index <- ctx.indices().value()) {
      selection.addIndex(visit(index))
    }
  }

  override def visitSelectAll(ctx: FlowParser.SelectAllContext): ExprDef.Builder = newBuilder(ctx) { builder =>
    builder.getSelectBuilder.setFrom(visit(ctx.from))
  }

  override def visitSlice(ctx: FlowParser.SliceContext): ExprDef.Builder = newBuilder(ctx) { builder =>
    val slice = builder.getSliceBuilder.setFrom(visit(ctx.from))
    if (ctx.begin != null) {
      slice.setBegin(visit(ctx.begin))
    } else {
      slice.getBeginBuilder.getLiteralBuilder // null
    }
    if (ctx.end != null) {
      slice.setEnd(visit(ctx.end))
    } else {
      slice.getEndBuilder.getLiteralBuilder // null
    }
  }

  override def visitFold(ctx: FlowParser.FoldContext): ExprDef.Builder = newBuilder(ctx) { builder =>
    builder.getFoldBuilder
      .setFrom(visit(ctx.from))
  }

  override def visitProject(ctx: FlowParser.ProjectContext): ExprDef.Builder = newBuilder(ctx) { builder =>
    builder.getProjectBuilder
      .setFrom(visit(ctx.from))
      .setIndex(visit(ctx.expression))
  }

  override def visitExpand(ctx: FlowParser.ExpandContext): ExprDef.Builder = newBuilder(ctx) { builder =>
    builder.getExpandBuilder
      .setFrom(visit(ctx.from))
      .setIndex(visit(ctx.expression))
  }

  override def visitQuery(ctx: FlowParser.QueryContext): ExprDef.Builder = newBuilder(ctx) { builder =>
    builder.getQueryBuilder
      .setFrom(visit(ctx.from))
      .setPredicate(visit(ctx.predicate()))
  }

  override def visitTransform(ctx: FlowParser.TransformContext): ExprDef.Builder = newBuilder(ctx) { builder =>
    builder.getTransformBuilder
      .setFrom(visit(ctx.from))
      .setExpression(visit(ctx.expression()))
  }

  override def visitTranspose(ctx: FlowParser.TransposeContext): ExprDef.Builder = newBuilder(ctx) { builder =>
    builder.getTransposeBuilder
      .setFrom(visit(ctx.from))
  }

  override def visitNot(ctx: FlowParser.NotContext): ExprDef.Builder = {
    newUnary(ctx, OP_NOT)
  }

  override def visitAnd(ctx: FlowParser.AndContext): ExprDef.Builder = {
    newBinary(ctx, OP_AND)
  }

  override def visitOr(ctx: FlowParser.OrContext): ExprDef.Builder = {
    newBinary(ctx, OP_OR)
  }

  override def visitToBoolean(ctx: FlowParser.ToBooleanContext): ExprDef.Builder = {
    newUnary(ctx, OP_TO_BOOLEAN)
  }

  override def visitEqualOrNot(ctx: FlowParser.EqualOrNotContext): ExprDef.Builder = {
    ctx.op.getText match {
      case "==" => newBinary(ctx, OP_EQ)
      case "!=" => newBinary(ctx, OP_NE)
    }
  }

  override def visitCompare(ctx: FlowParser.CompareContext): ExprDef.Builder = {
    ctx.op.getText match {
      case ">" => newBinary(ctx, OP_GT)
      case ">=" => newBinary(ctx, OP_GE)
      case "<" => newBinary(ctx, OP_LT)
      case "<=" => newBinary(ctx, OP_LE)
    }
  }

  override def visitIn(ctx: FlowParser.InContext): ExprDef.Builder = {
    newBinary(ctx, OP_IN)
  }

  override def visitNotIn(ctx: FlowParser.NotInContext): ExprDef.Builder = {
    newBinary(ctx, OP_NOT_IN)
  }

  override def visitMinus(ctx: FlowParser.MinusContext): ExprDef.Builder = {
    newUnary(ctx, OP_MINUS)
  }

  override def visitMulDiv(ctx: FlowParser.MulDivContext): ExprDef.Builder = {
    ctx.op.getText match {
      case "*" => newBinary(ctx, OP_MULTIPLY)
      case "/" => newBinary(ctx, OP_DIVIDE)
      case "%" => newBinary(ctx, OP_MOD)
    }
  }

  override def visitAddSub(ctx: FlowParser.AddSubContext): ExprDef.Builder = {
    ctx.op.getText match {
      case "+" => newBinary(ctx, OP_ADD)
      case "-" => newBinary(ctx, OP_SUBTRACT)
    }
  }

  private def visitString(node: TerminalNode): String = {
    val reader = new JsonReader(new StringReader(node.getText))
    reader.nextString()
  }

  private def newUnary(ctx: ParserRuleContext, op: Operator) = newBuilder(ctx) { builder=>
    val children: Seq[ExprDef.Builder] = ctx.children.collect {
      case ctx: FlowParser.ValueContext => visit(ctx)
      case ctx: FlowParser.PredicateContext => visit(ctx)
      case ctx: FlowParser.ExpressionContext => visit(ctx)
    }

    require(children.size == 1)
    builder.getUnaryBuilder.setOp(op).setChild(children.head)
  }

  private def newBinary(ctx: ParserRuleContext, op: Operator) = newBuilder(ctx) { builder=>
    val children: Seq[ExprDef.Builder] = ctx.children.collect {
      case ctx: FlowParser.ValueContext => visit(ctx)
      case ctx: FlowParser.PredicateContext => visit(ctx)
      case ctx: FlowParser.ExpressionContext => visit(ctx)
    }

    require(children.size == 2)
    builder.getBinaryBuilder.setOp(op).setLeft(children.head).setRight(children.last)
  }

  private def newBuilder(ctx: ParserRuleContext)(f: ExprDef.Builder => Unit) = {
    val builder = ExprDef.newBuilder()

    val interval = new Interval(ctx.start.getStartIndex, ctx.stop.getStopIndex)
    builder.setCode(ctx.start.getInputStream.getText(interval))

    f(builder)
    builder
  }
}
