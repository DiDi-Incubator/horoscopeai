/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.runtime.expression

import akka.http.scaladsl.server.Route
import com.didichuxing.horoscope.runtime._

import scala.languageFeature.implicitConversions
import scala.util.Try

class BuiltIn(
  val functions: Map[String, BuiltIn.FuncImpl],
  val methods: Map[String, BuiltIn.MethodImpl]
) {
  import BuiltIn._
  def toBuilder(): BuiltIn.Builder = new Builder().mergeFrom(this)

  def api: Route = {
    import akka.http.scaladsl.server.Directives._
    import Implicits._

    concat(
      path("evaluate") {
        parameters("expression", "context".as[Value]) { (expression, context) =>
          val result = Expression(expression)(this).apply(context.as[ValueDict])
          complete(result)
        }
      }
    )
  }
}

object BuiltIn {
  type FuncImpl = ValueList => Value // args => return-value

  type MethodImpl = (Value, ValueList) => Value // (object, args) => return-value

  class Builder {
    import shapeless.ops.function._

    import scala.reflect.runtime.universe.TypeTag

    private val functions = Map.newBuilder[String, FuncImpl]

    private val methods = Map.newBuilder[String, MethodImpl]

    def addFunction(name: String, impl: FuncImpl): Builder = {
      functions += name -> impl
      this
    }

    def addFunction[F, O, H, R <: Value](name: String)(func: F)(
      implicit
      wrapFunc: FnToProduct.Aux[F, H => O],
      toArgs: Value.To[H],
      wrapResult: Value.From.Aux[O, R]
    ): Builder = addFunction(name, list => {
      val f = wrapFunc(func)
      val args = Try(toArgs(list)).getOrElse(throw new IllegalArgumentException(
        s"function $name could not take $list as arguments"
      ))
      val result = f(args)
      Try(wrapResult(result)).getOrElse(throw new IllegalArgumentException(
        s"return value of $name is $result, which could not be converted to Value"
      ))
    })

    def addMethod(name: String, impl: MethodImpl): Builder = {
      methods += name -> impl
      this
    }

    def addMethod[T: TypeTag, F, H, O, R <: Value](name: String)(method: T => F)(
      implicit
      toObject: Value.To[T],
      wrapFunc: FnToProduct.Aux[F, H => O],
      toArgs: Value.To[H],
      wrapResult: Value.From.Aux[O, R]
    ): Builder = addMethod(name, (value, list) => {
      val obj = Try(toObject(value)).getOrElse(throw new IllegalArgumentException(
        s"$value could not be converted to type ${implicitly[TypeTag[T]].tpe}"
      ))
      val f = wrapFunc(method(obj))
      val args = Try(toArgs(list)).getOrElse(throw new IllegalArgumentException(
        s"method $name could not take $list as arguments"
      ))
      val result = f(args)
      Try(wrapResult(result)).getOrElse(throw new IllegalArgumentException(
        s"return value of $name is $result, which could not be converted to Value"
      ))
    })

    def build(): BuiltIn = {
      new BuiltIn(functions.result(), methods.result())
    }

    def mergeFrom(builtIn: BuiltIn): this.type = {
      builtIn.functions.foreach(e => addFunction(e._1, e._2))
      builtIn.methods.foreach(e => addMethod(e._1, e._2))
      this
    }
  }
}

