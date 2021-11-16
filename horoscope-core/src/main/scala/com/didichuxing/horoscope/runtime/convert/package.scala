/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.runtime

import scala.reflect.runtime.universe._

package object convert {
  def newFrom[T, V <: Value](fn: T => V): Value.From.Aux[T, V] = new Value.From[T] {
    type ValueType = V

    def apply(elem: T): V = fn(elem)
  }

  def newTo[T: TypeTag](fn: PartialFunction[Value, T]): Value.To[T] = new Value.To[T] {
    def apply(elem: Value): T = {
      try {
        fn(elem)
      } catch {
        case cause: Exception =>
          val errorMessage = s"Can not cast `$elem` to type ${implicitly[TypeTag[T]].tpe}"
          throw ValueCastException(errorMessage, cause)
      }
    }
  }

  trait Implicits
    extends BasicConvertible
      with GsonConvertible
      with ProtobufConvertible
      with JythonConvertible
      with FlatBufferConvertible

}
