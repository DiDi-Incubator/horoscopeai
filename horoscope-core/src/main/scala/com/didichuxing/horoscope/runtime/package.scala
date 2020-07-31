/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope

import com.didichuxing.horoscope.runtime.convert.DefaultGson
import com.didichuxing.horoscope.runtime.expression.DefaultBuiltIn

import scala.language.implicitConversions

// scalastyle:off
package object runtime {
  sealed trait Wildcard

  object * extends Wildcard

  trait LowPriorityImplicits {
    implicit def asDocument(value: Value): Document = value.asInstanceOf[Document]

    implicit def mkValueList(seq: Seq[Value]): ValueList = new SimpleList(seq)

    implicit def mkValueDict(map: Map[String, Value]): ValueDict = new SimpleDict(map)

    implicit def mkBoolean(boolean: Boolean): BooleanValue = BooleanValue(boolean)

    implicit def mkText(string: String): Text = Text(string)

    implicit def mkNumber(bigDecimal: BigDecimal): NumberValue = NumberValue(bigDecimal)

    implicit def mkBinary(bytes: Array[Byte]): Binary = Binary(bytes)
  }

  object Implicits
    extends LowPriorityImplicits
      with DefaultBuiltIn
      with DefaultGson
}
