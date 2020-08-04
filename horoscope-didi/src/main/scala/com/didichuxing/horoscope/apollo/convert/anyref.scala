/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: huchengyi@didiglobal.com
 * Description:
 */
package com.didichuxing.horoscope.apollo.convert

import com.didichuxing.horoscope.runtime._
import java.lang.{Integer => JInteger}
import java.lang.{Long => JLong}
import java.lang.{Float => JFloat}
import java.lang.{Double => JDouble}
import java.lang.{Byte => JByte}
import java.lang.{Short => JShort}
import java.lang.{Boolean => JBoolean}
import java.lang.{String => JString}

import scala.collection.JavaConverters._
import scala.collection.mutable

trait AnyRefConvertible {
  implicit val fromAnyRef: Value.From.Aux[AnyRef, Value] = new AnyRefConverter
}

class AnyRefConverter extends Value.From[AnyRef] {
  type ValueType = Value

  override def apply(obj: AnyRef): Value = fromAnyRef(obj)

  def fromAnyRef(obj: Object): Value = {
    obj match {
      case _: java.lang.Void => NULL
      case null => NULL
      case i: JInteger => NumberValue(BigDecimal(i))
      case i: JLong => NumberValue(BigDecimal(i))
      case i: JFloat => NumberValue(BigDecimal(i))
      case i: JDouble => NumberValue(BigDecimal(i))
      case i: JShort => NumberValue(BigDecimal(i.toInt))
      case i: JByte => NumberValue(BigDecimal(i.toInt))
      case b: JBoolean => BooleanValue(b)
      case s: JString => Text(s)
      case xs: Array[Object] =>
        val l = xs.map(o => fromAnyRef(o))
        new SimpleList(l)
      case xs: java.util.List[Object] =>
        val l = xs.asScala.map(o => fromAnyRef(o))
        new SimpleList(l)
      case xs: Seq[Object] =>
        val l = xs.map(o => fromAnyRef(o))
        new SimpleList(l)
      case xm: java.util.Map[String, Object] =>
        val m = xm.asScala.map { case (k, v) => k -> fromAnyRef(v) }
        new SimpleDict(m.toMap)
      case xm: Map[String, Object] =>
        val m = xm.map { case (k, v) => k -> fromAnyRef(v) }
        new SimpleDict(m)
      case _ =>
        throw new IllegalArgumentException(s"Can't cast ${obj} to Value")
    }
  }

}
