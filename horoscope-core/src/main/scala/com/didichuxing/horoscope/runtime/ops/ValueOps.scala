/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.runtime.ops

import com.didichuxing.horoscope.runtime._
import com.google.gson.Gson

class ValueOps(val repr: Value) extends AnyVal {
  def toJson(implicit gson: Gson): String = gson.toJson(repr)

  def as[T](implicit to: Value.To[T]): T = to(repr)

  def valueType: String = repr match {
    case NULL => "null"
    case _: NumberValue => "number"
    case _: BooleanValue => "boolean"
    case _: Text => "string"
    case _: Binary => "blob"
    case _: ValueList => "array"
    case _: ValueDict => "object"
  }
}

