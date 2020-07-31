/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.runtime.ops

import com.didichuxing.horoscope.runtime.NumberValue

import scala.language.implicitConversions

trait NumericImplicits {
  implicit object NumberValueNumeric extends NumberValueIsFractional with NumberOrdering

  object NumberValueAsIfIntegral extends NumberValueAsIfIntegral with NumberOrdering
}

trait NumberValueIsConflicted extends Numeric[NumberValue] {
  def plus(x: NumberValue, y: NumberValue): NumberValue = x + y
  def minus(x: NumberValue, y: NumberValue): NumberValue = x - y
  def times(x: NumberValue, y: NumberValue): NumberValue = x * y
  def negate(x: NumberValue): NumberValue = -x
  def fromInt(x: Int): NumberValue = NumberValue(x)
  def toInt(x: NumberValue): Int = x.intValue
  def toLong(x: NumberValue): Long = x.longValue
  def toFloat(x: NumberValue): Float = x.floatValue
  def toDouble(x: NumberValue): Double = x.doubleValue

  @inline implicit def toUnderlying(value: NumberValue): BigDecimal = value.underlying
  @inline implicit def toValue(underlying: BigDecimal): NumberValue = NumberValue(underlying)
}

trait NumberValueIsFractional extends NumberValueIsConflicted with Fractional[NumberValue] {
  def div(x: NumberValue, y: NumberValue): NumberValue = x / y
}

trait NumberValueAsIfIntegral extends NumberValueIsConflicted with Integral[NumberValue] {
  def quot(x: NumberValue, y: NumberValue): NumberValue = x quot y
  def rem(x: NumberValue, y: NumberValue): NumberValue = x remainder y
}

