/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.runtime.expression

import com.didichuxing.horoscope.core.FlowDslMessage.{ExprDef, Operator}
import com.didichuxing.horoscope.runtime.{NULL, _}

import scala.collection.mutable

trait Operators extends OperatorFactory {
  import Implicits._
  import Operator._

  shortcut(OP_AND)(And)

  shortcut(OP_OR)(Or)

  unary(OP_TO_BOOLEAN) {
    case NULL => false
    case Text(text) => text.nonEmpty
    case NumberValue(value) => value != 0
    case BooleanValue(bool) => bool
    case document: Document => document.children.nonEmpty
  }

  unary(OP_MINUS) {
    case NumberValue(x) => -x
  }

  unary(OP_NOT) {
    case BooleanValue(value) => !value
  }

  binary(OP_EQ) {
    case (left, right) => left == right
  }

  binary(OP_NE) {
    case (left, right) => left != right
  }

  binary(OP_LT) {
    case (NumberValue(left), NumberValue(right)) => left < right
    case (Text(left), Text(right)) => left < right
    case (_, NULL) => false
    case (NULL, _) => false
  }

  binary(OP_LE) {
    case (NumberValue(left), NumberValue(right)) => left <= right
    case (Text(left), Text(right)) => left <= right
    case (_, NULL) => false
    case (NULL, _) => false
  }

  binary(OP_GT) {
    case (NumberValue(left), NumberValue(right)) => left > right
    case (Text(left), Text(right)) => left > right
    case (_, NULL) => false
    case (NULL, _) => false
  }

  binary(OP_GE) {
    case (NumberValue(left), NumberValue(right)) => left >= right
    case (Text(left), Text(right)) => left >= right
    case (_, NULL) => false
    case (NULL, _) => false
  }

  binary(OP_IN) {
    case (value: Value, list: ValueList) => list.children.contains(value)
    case (Text(name), dict: ValueDict) => dict.at(name).nonEmpty
  }

  binary(OP_NOT_IN) {
    case (value: Value, list: ValueList) => !list.children.contains(value)
    case (Text(name), dict: ValueDict) => dict.at(name).isEmpty
  }

  binary(OP_ADD) {
    case (NULL, NULL) => NULL
    case (NumberValue(left), NumberValue(right)) => left + right
    case (Text(left), Text(right)) => left + right
    case (Text(left), NumberValue(right)) => left + right.toString()
    case (NumberValue(left), Text(right)) => left.toString() + right
    case (left: TableView, right: TableView) if left.dims == right.dims => left.update(right.impl)
    case (ValueList(left), ValueList(right)) => left ++ right
  }

  binary(OP_SUBTRACT) {
    case (NULL, NULL) => NULL
    case (NumberValue(left), NumberValue(right)) => left - right
    case (ValueList(left), ValueList(right)) => left.filterNot(right.contains)
  }

  binary(OP_MULTIPLY) {
    case (NumberValue(left), NumberValue(right)) => left * right
    case (Text(left), NumberValue(right)) => left * right.intValue()
  }

  binary(OP_DIVIDE) {
    case (NumberValue(left), NumberValue(right)) => left / right
  }

  binary(OP_MOD) {
    case (NumberValue(left), NumberValue(right)) => left % right
  }
}

trait OperatorFactory extends ExpressionFactory {
  import ExprDef.ExpressionCase.{BINARY, UNARY}
  import Expression._

  define(UNARY, _.getUnary).using(unary => operators(unary.getOp))
  define(BINARY, _.getBinary).using(binary => operators(binary.getOp))

  val operators: mutable.Map[Operator, Factory] = mutable.Map()

  def shortcut(op: Operator)(impl: (ExprDef, Expression, Expression) => Expression): Unit = {
    operators.update(op, definition => {
      val binary = definition.getBinary
      impl(definition, build(binary.getLeft), build(binary.getRight))
    })
  }

  def unary(op: Operator)(impl: UnaryImpl): Unit = {
    operators.update(op, definition => {
        val unary = definition.getUnary
        UnaryExpression(definition, build(unary.getChild), impl)
    })
  }

  def binary(op: Operator)(impl: BinaryImpl): Unit = {
    operators.update(op, definition => {
      val binary = definition.getBinary
      BinaryExpression(definition, build(binary.getLeft), build(binary.getRight), impl)
    })
  }
}

