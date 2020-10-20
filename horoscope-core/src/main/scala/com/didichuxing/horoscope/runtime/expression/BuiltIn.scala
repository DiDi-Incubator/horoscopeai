/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.runtime.expression

import akka.http.scaladsl.server.Route
import com.didichuxing.horoscope.runtime._
import com.didichuxing.horoscope.runtime.expression.BuiltIn.{FuncImpl, MethodImpl}
import com.didichuxing.horoscope.runtime.expression.SimpleBuiltIn.Builder

import scala.languageFeature.implicitConversions
import scala.util.Try

trait BuiltIn {

  def functions: scala.collection.Map[String, BuiltIn.FuncImpl]

  def methods: scala.collection.Map[String, BuiltIn.MethodImpl]

  def api: Route = _.reject()

  def getFunction(namespace: String, name: String): Option[BuiltIn.FuncImpl] = {
    namespace match {
      case "" =>
        Try(functions.filter {
          case (udfPath, _) =>
            val (_, udfName) = path2UDFName(udfPath)
            udfName.equals(name)
        }.toList.sortBy(_._1.length).last._2).toOption
      case _: String =>
        Try(functions.filter {
          case (udfPath, _) =>
            val (udfNS, udfName) = path2UDFName(udfPath)
            namespace.startsWith(udfNS) && udfName.equals(name)
        }.toList.sortBy(_._1.length).last._2).toOption
      case _ =>
        None
    }
  }

  def getMethod(namespace: String, name: String): Option[BuiltIn.MethodImpl] = {
    namespace match {
      case "" =>
        Try(methods.filter {
          case (udfPath, _) =>
            val (_, udfName) = path2UDFName(udfPath)
            udfName.equals(name)
        }.toList.sortBy(_._1.length).last._2).toOption
      case _: String =>
        Try(methods.filter {
          case (udfPath, _) =>
            val (udfNS, udfName) = path2UDFName(udfPath)
            namespace.startsWith(udfNS) && udfName.equals(name)
        }.toList.sortBy(_._1.length).last._2).toOption
      case _ =>
        None
    }
  }

  def path2UDFName(path: String): (String, String) = {
    val namePath = path.replaceAll("/(functions|methods)", "").drop(1)
    val n = namePath.lastIndexOf("/")
    if (n == -1) {
      ("/", namePath)
    } else {
      (s"/${namePath.take(n + 1)}", namePath.drop(n + 1))
    }
  }

}

object BuiltIn {
  type FuncImpl = ValueList => Value // args => return-value

  type MethodImpl = (Value, ValueList) => Value // (object, args) => return-value
}

class SimpleBuiltIn(val functions: Map[String, BuiltIn.FuncImpl],
                    val methods: Map[String, BuiltIn.MethodImpl]) extends BuiltIn {

  def toBuilder(): Builder = new Builder().mergeFrom(this)

  override def api: Route = {
    import akka.http.scaladsl.server.Directives._
    import Implicits._

    concat(
      path("evaluate") {
        (parameters("expression", "context".as[Value]) |
          formFields("expression", "context".as[Value])) { (expression, context) =>
          val result = Expression(expression)(this).apply(context.as[ValueDict])
          complete(result)
        }
      }
    )
  }
}

object SimpleBuiltIn {

  class Builder {

    import shapeless.ops.function._

    import scala.reflect.runtime.universe.TypeTag

    private val functions = Map.newBuilder[String, FuncImpl]

    private val methods = Map.newBuilder[String, MethodImpl]

    def addFunction(name: String, impl: FuncImpl): Builder = {
      val path = if (name.startsWith("/")) name else s"/$name"
      functions += path -> impl
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
      val path = if (name.startsWith("/")) name else s"/$name"
      methods += path -> impl
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

    def build(): SimpleBuiltIn = {
      new SimpleBuiltIn(functions.result(), methods.result())
    }

    def mergeFrom(builtIn: BuiltIn): this.type = {
      builtIn.functions.foreach(e => addFunction(e._1, e._2))
      builtIn.methods.foreach(e => addMethod(e._1, e._2))
      this
    }
  }

}
