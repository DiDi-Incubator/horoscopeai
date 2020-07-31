/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.runtime

import com.didichuxing.horoscope.core.FlowRuntimeMessage
import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowInstance.Choose
import com.didichuxing.horoscope.core.FlowRuntimeMessage.{FlowInstance, FlowValue, TraceVariable}

import scala.languageFeature.implicitConversions
import com.didichuxing.horoscope.runtime.convert.ValueTypeAdapter
import com.didichuxing.horoscope.util.Logging
import com.google.gson.{Gson, GsonBuilder}
import com.google.protobuf.ByteString
import org.scalatest.{FunSuite, Matchers}

// scalastyle:off
class ValueSuite extends FunSuite with Matchers with Logging {

  import ValueSuite._

  implicit val gson: Gson = new GsonBuilder()
    .registerTypeHierarchyAdapter(classOf[Value], new ValueTypeAdapter)
    .setPrettyPrinting()
    .create()

  def show(value: Value): Unit = {
    printf("%s\n", value.toString)
  }

  test("convert from scala type") {
    //    show(Value(Array("a", "b", "c")))
    //
    //    show(Value(1 :: 2 :: 3 :: Nil))
    //
    //    show(Value(Map("a" -> 1, "b" -> 2)))
    //
    val c = C("hello", 1, Seq(C("world", 0), C("!", 3)))

    val v: Value = Value(c)
    println(v.toJson)

    val t: Value = Value((c, c, c))
    println(t.toJson)
  }

  import shapeless._

  implicit def productToG[T, Repr <: HList](
                                             implicit
                                             notTuple: Refute[IsTuple[T]],
                                             generic: LabelledGeneric.Aux[T, Repr]
                                             //    tag: TypeTag[Repr]
                                           ): GTest.Aux[T, Repr] = GTest.create { t =>
    val repr = generic.to(t)
    println(generic)
    repr
  }

  implicit def tupleToG[T, Repr <: HList](
                                           implicit
                                           isTuple: IsTuple[T],
                                           generic: Generic.Aux[T, Repr]
                                         ): GTest.Aux[T, Repr] = GTest.create { t =>
    val repr = generic.to(t)
    println(generic)
    repr
  }

  test("generic") {
    val c = C("hello", 1)
    val v: Value = Value(c)
    println(v.as[C])

    val t: Value = Value(("a", "he", true))
    t.as[(String, String, Boolean)] shouldBe("a", "he", true)
  }

  test("lt") {
    import Ordering.Implicits._

    val i: Int = 3
    val f: Double = 2.5

    val b = Value(i) < Value(f)
    println(b)
  }

  test("type") {
    import syntax.std.function._

    import scala.reflect.runtime.universe.TypeTag

    val f = (a: Int, b: String) => b * a
    val gf = f.toProduct
    val tag = implicitly[TypeTag[gf.type]]
    println()
  }

  test("numeric") {
    val n = Value(BigDecimal(1))
    println(n)
  }

  test("ordering") {
    import Ordering.Implicits._

    val s0 = Text("a")
    val s1 = Text("b")

    println(s0 < s1)
  }

  test("max") {
    //    import scala.math.Ordering.Implicits._
    //    import scala.Numeric.Implicits._

    val test: Seq[Int] = Seq(1, -2, 5, 3)
    println(test.max)
  }

  test("register function") {
    val builder = new Builder()
    builder.addFunction("test") {
      (d: Double) => d.toLong
    }
  }

  test("flow value converter") {
    val flowValueDict = FlowValue.Dict.newBuilder()
      .putChild("k1", FlowValue.newBuilder().setBinary(ByteString.copyFrom("ab".getBytes())).build())
      .putChild("k2", FlowValue.newBuilder().setIntegral(3).build())
      .putChild("k3", FlowValue.newBuilder().setBoolean(false).build())
      .putChild("k4", FlowValue.newBuilder().setFractional(1.9).build())
      .putChild("k5", FlowValue.newBuilder().setText("abc").build())
      .putChild("k6", FlowValue.newBuilder().setList(
        FlowValue.List.newBuilder().addChild(FlowValue.newBuilder().setIntegral(1).build())).build()
      )
    val flowValue = FlowValue.newBuilder().setDict(flowValueDict).build()
    val v: Value = Value(flowValue)
    println("json==" + v.toJson)
  }

  test("flow instance converter") {
    val flowValueDict = FlowValue.Dict.newBuilder()
      .putChild("k1", FlowValue.newBuilder().setBinary(ByteString.copyFrom("ab".getBytes())).build())
      .putChild("k2", FlowValue.newBuilder().setIntegral(3).build())
      .putChild("k3", FlowValue.newBuilder().setBoolean(false).build())
      .putChild("k4", FlowValue.newBuilder().setFractional(1.9).build())
      .putChild("k5", FlowValue.newBuilder().setText("abc").build())
      .putChild("k6", FlowValue.newBuilder().setList(
        FlowValue.List.newBuilder().addChild(FlowValue.newBuilder().setIntegral(1).build())).build()
      )
    val flowValue = FlowValue.newBuilder().setDict(flowValueDict).build()
    val instance = FlowInstance.newBuilder()
      .setFlowId("id")
      .setEvent(FlowRuntimeMessage.FlowEvent.newBuilder()
        .setEventId("e1")
        .setTraceId("t1")
        .setFlowName("f1")
        .putArgument("@s1", TraceVariable.newBuilder().setValue(flowValue).build())
      )
      .addChoose(Choose.newBuilder.setChoice("if1"))
      .addChoose(Choose.newBuilder.setChoice("if2"))
      .setStartTime(System.currentTimeMillis())
      .setEndTime(System.currentTimeMillis())
      .build()
    val v: Value = Value(instance)
    println(v.toJson)
  }

  test("map to value") {
    val m: Value = Value(Map("a" -> 1, "b" -> 2))
    println(m.as[FlowValue].toString)
  }
}

object ValueSuite {

  import scala.reflect.runtime.universe.TypeTag

  class Builder {

    import shapeless.ops.function._

    def addFunction[F, O, H, R <: Value](name: String)(func: F)(
      implicit
      wrapFunc: FnToProduct.Aux[F, H => O],
      //      tag: TypeTag[H],
      toArgs: Value.To[H]
      //      wrapResult: Value.From.Aux[O, R]
    ): Unit = {
      //      println(tag)
    }
  }

  case class C(a: String, b: Int, c: Seq[C] = Nil) {
    def func(args: Seq[String]): Boolean = {
      true
    }
  }

  trait GTest[T] {
    type Out

    def apply(t: T): Out
  }

  object GTest {
    // scalastyle:off
    type Aux[T, O] = GTest[T] {type Out = O}

    def apply[T](implicit inst: GTest[T]): GTest[T] = inst

    def create[T, O](fn: T => O): Aux[T, O] = new GTest[T] {
      type Out = O

      def apply(t: T): O = fn(t)
    }
  }

}

