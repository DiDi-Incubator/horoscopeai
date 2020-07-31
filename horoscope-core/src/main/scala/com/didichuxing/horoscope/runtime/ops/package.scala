package com.didichuxing.horoscope.runtime

import scala.language.implicitConversions

package object ops {
  trait Implicits extends OrderingImplicits with NumericImplicits {
    implicit def mkValueOps(value: Value): ValueOps = new ValueOps(value)

    implicit def mkDictOps(dict: ValueDict): DictOps = new DictOps(dict)

    implicit def mkListOps(list: ValueList): ListOps = new ListOps(list)

    implicit def mkTextOps(text: Text): String = text.underlying

    implicit def mkBooleanOps(bool: BooleanValue): Boolean = bool.underlying

    implicit def mkNumberOps(number: NumberValue): BigDecimal = number.underlying

    implicit def mkBinaryOps(binary: Binary): Array[Byte] = binary.underlying
  }
}
