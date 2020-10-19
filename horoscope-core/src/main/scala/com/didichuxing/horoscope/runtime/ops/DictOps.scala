/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.runtime.ops

import com.didichuxing.horoscope.runtime._
import com.didichuxing.horoscope.runtime.expression.{BuiltIn, Expression}

class DictOps(val repr: ValueDict) extends AnyVal {

  def eval(expression: String)(implicit builtin: BuiltIn): Value = {
    Expression(expression).apply(repr)
  }

  def eval(namespace: String, expression: String)(implicit builtin: BuiltIn): Value = {
    Expression(namespace, expression).apply(repr)
  }
}
