/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.runtime.expression

import com.didichuxing.horoscope.core.FlowDslMessage.ExprDef
import com.didichuxing.horoscope.runtime._

import scala.collection.mutable
import scala.reflect.ClassTag

trait ExpressionFactory {
  import Expression._
  import ExprDef.ExpressionCase

  type Factory = ExprDef => Expression

  def build(definition: ExprDef): Expression = {
    factories(definition.getExpressionCase)(definition)
  }

  val factories: mutable.Map[ExpressionCase, Factory] = mutable.Map()

  def define[M](enum: ExpressionCase, extract: ExprDef => M): Stub[M] = new Stub(enum, extract)

  class Stub[M](enum: ExpressionCase, extract: ExprDef => M) {
    def using(proxy: M => Factory): Unit = register { definition =>
      proxy(extract(definition))(definition)
    }

    def function(
      inputs: M => Iterable[ExprDef]
    )(impl: M => FunctionImpl): Unit = register { definition =>
      val message = extract(definition)
      FunctionExpression(definition, inputs(message).map(build).toSeq, impl(message))
    }

    def method[V <: Value : ClassTag](
      from: M => ExprDef, args: M => Iterable[ExprDef] = _ => Nil
    )(impl: M => MethodImpl[V]): Unit = register { definition =>
      val message = extract(definition)
      MethodExpression(definition, build(from(message)), args(message).map(build).toSeq, impl(message))
    }

    def monad(
      from: M => ExprDef, func: M => ExprDef
    )(impl: M => MonadImpl): Unit = register { definition =>
      val message = extract(definition)
      MonadExpression(definition, build(from(message)), build(func(message)), impl(message))
    }

    private def register(body: => Factory): Unit = {
      factories += enum -> body
    }
  }
}

