/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.runtime.expression

import com.didichuxing.horoscope.core.FlowDslMessage
import com.didichuxing.horoscope.core.FlowDslMessage.ExprDef
import com.didichuxing.horoscope.dsl.ExpressionParser
import com.didichuxing.horoscope.runtime._

import scala.reflect.ClassTag

object Expression {
  def apply(text: String)(implicit builtin: BuiltIn): Expression = {
    val parser = new ExpressionParser()
    val builder = new ExpressionBuilder(builtin)

    val definition = parser.parse(text)
    builder.build(definition)
  }

  type FunctionImpl =  ValueList => Value

  type MethodImpl[V <: Value] = (V, ValueList) => Value

  type MonadImpl = (Document, Document.Processor) =>  Value

  type UnaryImpl = PartialFunction[Value, Value]

  type BinaryImpl = PartialFunction[(Value, Value), Value]
}

import Expression._

sealed trait Expression extends Function[ValueDict, Value] {
  def definition: ExprDef

  def children: Seq[Expression]

  def references: Seq[Reference] = {
    this match {
      case ref: Reference if ref.name == "_" => Nil
      case ref: Reference => ref :: Nil
      case _ => children.flatMap(_.references)
    }
  }

  def evaluate(args: ValueDict): Value

  override def apply(context: ValueDict): Value = {
    evaluate(context) match {
      case table: TableView =>
        table.value
      case value: Value =>
        value
    }
  }

  override def toString(): String = definition.getCode
}

case class FunctionExpression(
  definition: ExprDef, children: Seq[Expression], impl: FunctionImpl
) extends Expression {
  override def evaluate(context: ValueDict): Value = impl(
    Value(children.map(_.apply(context))) // use apply to avoid nested TableView
  )
}

case class MethodExpression[V <: Value : ClassTag](
  definition: ExprDef, from: Expression, args: Seq[Expression], impl: MethodImpl[V]
) extends Expression {
  override val children: Seq[Expression] = from +: args

  override def evaluate(context: ValueDict): Value = {
    val clazz = implicitly[ClassTag[V]].runtimeClass
    from.evaluate(context) match {
      case value: Value if clazz.isInstance(value) =>
        impl(
          value.asInstanceOf[V],
          Value(args.map(_.apply(context))) // use apply to avoid nested TableView
        )
      case value: Value =>
        throw new IllegalArgumentException(
          s"${definition.getExpressionCase} expression need ${clazz.getSimpleName}, but ${value.valueType} is given"
        )
    }
  }
}

case class MonadExpression(
  definition: ExprDef, from: Expression, func: Expression, impl: MonadImpl
) extends Expression {
  override val children: Seq[Expression] = from :: func :: Nil

  override def evaluate(context: ValueDict): Value = {
    from.evaluate(context) match {
      case doc: Document =>
        impl(doc, {
          case (indices, value) =>
            // avoid to use func.evaluate, we should convert table to value
            func.apply(context.updated("#", indices).updated("_", value))
        })
      case value: Value =>
        throw new IllegalArgumentException(
          s"${definition.getExpressionCase} expression need document, but ${value.valueType} is given"
        )
    }
  }
}

case class UnaryExpression(
  definition: ExprDef, child: Expression, impl: UnaryImpl
) extends Expression {
  override val children: Seq[Expression] = child :: Nil

  override def evaluate(context: ValueDict): Value = {
    val argument = child.evaluate(context)
    impl.applyOrElse[Value, Value](argument,  _ => throw new IllegalArgumentException(
      s"${definition.getUnary.getOp} do not accept ${argument.valueType}"
    ))
  }
}

case class BinaryExpression(
  definition: ExprDef, left: Expression, right: Expression, impl: BinaryImpl
) extends Expression {
  override val children: Seq[Expression] = left :: right :: Nil

  override def evaluate(context: ValueDict): Value = {
    val arg0 = left.evaluate(context)
    val arg1 = right.evaluate(context)
    impl.applyOrElse[(Value, Value), Value]((arg0, arg1), _ => throw new IllegalArgumentException(
      s"${definition.getBinary.getOp} do not accept ${arg0.valueType} and ${arg1.valueType}"
    ))
  }
}

// Reference/And/Or need to be understand by optimizer, so they can have specialized implementation

case class Reference(definition: ExprDef) extends Expression {
  val message: FlowDslMessage.Reference = definition.getReference

  val name: String = message.getName

  val scope: Option[String] = if (message.hasScope) {
    Some(message.getScope)
  } else {
    None
  }

  val identifier: String = if (message.hasScope) {
    s"${message.getScope}->$name"
  } else {
    name
  }

  override val children: Seq[Expression] = Nil

  override def evaluate(context: ValueDict): Value = context.visit(identifier)
}

case class And(
  definition: ExprDef, left: Expression, right: Expression
) extends Expression {
  override val children: Seq[Expression] = left :: right :: Nil

  override def evaluate(context: ValueDict): Value = {
    BooleanValue(left.evaluate(context).as[Boolean] && right.evaluate(context).as[Boolean])
  }
}

case class Or(
  definition: ExprDef, left: Expression, right: Expression
) extends Expression {
  override val children: Seq[Expression] = left :: right :: Nil

  override def evaluate(context: ValueDict): Value = {
    BooleanValue(left.evaluate(context).as[Boolean] || right.evaluate(context).as[Boolean])
  }
}
