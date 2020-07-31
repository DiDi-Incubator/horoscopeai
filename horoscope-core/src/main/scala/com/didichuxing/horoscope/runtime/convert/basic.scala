/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.runtime.convert

import com.didichuxing.horoscope.runtime.Value.{From, To}
import com.didichuxing.horoscope.runtime._
import shapeless._
import shapeless.labelled.FieldType

import scala.collection.generic.CanBuildFrom
import scala.language.{higherKinds, implicitConversions}

trait BasicConvertible {
  import Numeric.Implicits._
  import scala.reflect.runtime.universe.TypeTag
  import scala.reflect.ClassTag

  implicit def fromValue[V <: Value]: From.Aux[V, V] = newFrom(v => v)

  implicit val fromString: From.Aux[String, Text] = newFrom(Text)

  implicit val fromBinary: From.Aux[Array[Byte], Binary] = newFrom(Binary)

  implicit val fromBoolean: From.Aux[Boolean, BooleanValue] = newFrom(BooleanValue)

  implicit val fromBigInt: From.Aux[BigInt, NumberValue] = newFrom(i => NumberValue(BigDecimal(i)))

  implicit val fromBigDecimal: From.Aux[BigDecimal, NumberValue] = newFrom(d => NumberValue(d))

  implicit val fromNumberValue: From.Aux[NumberValue, NumberValue] = newFrom(n => n)

  implicit def fromIntegral[I: Integral]: From.Aux[I, NumberValue] = newFrom(i => NumberValue(i.toLong))

  implicit def fromFractional[F: Fractional]: From.Aux[F, NumberValue] = newFrom(f => NumberValue(f.toDouble()))

  implicit def fromScalaIterable[CC[_], T](
    implicit
    conv: CC[T] => Iterable[T],
    converter: Lazy[From[T]]
  ): From.Aux[CC[T], ValueList] = newFrom { iterable =>
    new SimpleList(iterable.view.map(lazily(converter)(_)).toSeq)
  }

  implicit def fromScalaOption[T](
    converter: Lazy[From[T]]
  ): From.Aux[Option[T], Value] = newFrom {
    case None => NULL
    case Some(item) => lazily(converter)(item)
  }

  implicit def fromScalaMap[CC, T](
    implicit
    conv: CC => Map[String, T],
    converter: Lazy[From[T]]
  ): From.Aux[CC, ValueDict] = newFrom { pairs =>
    new SimpleDict(pairs.mapValues(lazily(converter)(_)))
  }

  implicit def fromScalaProduct[P <: Product, Repr <: HList, KL <: HList, VL <: HList, RL <: HList](
    implicit
    notValue: <:!<[P, Value],
    generic: LabelledGeneric.Aux[P, Repr],
    fields: shapeless.ops.record.UnzipFields.Aux[Repr, KL, VL],
    keyToList: shapeless.ops.hlist.ToTraversable.Aux[KL, Iterable, Symbol],
    converter: Lazy[shapeless.ops.hlist.Mapper.Aux[ConvertToValue.type, VL, RL]],
    resultToList: shapeless.ops.hlist.ToTraversable.Aux[RL, Iterable, Value]
  ): From.Aux[P, Document] = newFrom { product =>
    val h = generic.to(product)
    val children = resultToList(lazily(converter)(fields.values(h)))

    if (product.productPrefix.startsWith("Tuple")) {
      new SimpleList(children)
    } else {
      val indices = keyToList(fields.keys()).map(_.name)
      new SimpleDict(indices, children)
    }
  }

  implicit def valueToValue[V <: Value: TypeTag : ClassTag]: To[V] = newTo {
    case value: Value if implicitly[ClassTag[V]].runtimeClass.isInstance(value) => value.asInstanceOf[V]
  }

  implicit val valueToText: To[String] = newTo {
    case NULL => null
    case Text(s) => s
  }

  implicit val valueToBoolean: To[Boolean] = newTo {
    case BooleanValue(b) => b
    case NULL => false
    case Text(text) => text.nonEmpty
    case NumberValue(value) => value != 0
    case document: Document => document.children.nonEmpty
  }

  implicit val valueToInt: To[Int] = newTo {
    case NumberValue(x) => x.toIntExact
  }

  implicit val valueToLong: To[Long] = newTo {
    case NumberValue(x) => x.toLongExact
  }

  implicit val valueToFloat: To[Float] = newTo {
    case NULL => Float.NaN
    case NumberValue(null) => Float.NaN
    case NumberValue(x) => x.toFloat
  }

  implicit val valueToDouble: To[Double] = newTo {
    case NULL => Double.NaN
    case NumberValue(null) => Double.NaN
    case NumberValue(x) => x.toDouble
  }

  implicit val valueToBigDecimal: To[BigDecimal] = newTo {
    case NULL => null
    case NumberValue(n) => n
  }

  implicit def valueToOption[T: TypeTag](
    implicit
    converter: Lazy[To[T]]
  ): To[Option[T]] = newTo {
    case NULL => None
    case item: Value => Some(converter.value(item))
  }

  implicit def listToScalaIterable[CC[_], T](
    implicit
    converter: Lazy[To[T]],
    cbf: CanBuildFrom[Nothing, T, CC[T]],
    typeTag: TypeTag[CC[T]]
  ): To[CC[T]] = newTo {
    case NULL =>
      null.asInstanceOf[CC[T]]
    case list: ValueList =>
      val builder = cbf()
      builder.sizeHint(list.children.size)
      for (value <- list.children) {
        builder += converter.value(value)
      }
      builder.result()
  }

  implicit def dictToScalaMap[CC[_, _], T](
    implicit
    converter: Lazy[To[T]],
    cbf: CanBuildFrom[Nothing, (String, T), CC[String, T]],
    typeTag: TypeTag[CC[String, T]]
  ): To[CC[String, T]] = newTo {
    case NULL =>
      null.asInstanceOf[CC[String, T]]
    case dict: ValueDict =>
      val builder = cbf()
      builder.sizeHint(dict.children.size)
      for ((name, value) <- dict.iterator) {
        builder += name -> converter.value(value)
      }
      builder.result()
  }

  implicit def dictToCaseClass[P <: Product : TypeTag, Repr <: HList](
    implicit
    notValue: <:!<[P, Value],
    notTuple: Refute[IsTuple[P]],
    generic: LabelledGeneric.Aux[P, Repr],
    factory: Lazy[RecordFactory[Repr]]
  ): To[P] = newTo {
    case NULL =>
      null.asInstanceOf[P]
    case dict: ValueDict =>
      generic.from(
        lazily(factory)(dict)
      )
  }

  implicit def listToScalaTuple[T <: Product : TypeTag, Repr <: HList, N <: Nat](
    implicit
    isTuple: IsTuple[T],
    generic: Generic.Aux[T, Repr],
    to: Lazy[To[Repr]]
  ): To[T] = newTo {
    case NULL =>
      null.asInstanceOf[T]
    case list: ValueList =>
      generic.from(lazily(to)(list))
  }

  implicit def listToHList[H <: HList : TypeTag, N <: Nat](
    implicit
    len: shapeless.ops.hlist.Length.Aux[H, N],
    n: shapeless.ops.nat.ToInt[N],
    factory: Lazy[HListFactory[H]]
  ): To[H] = newTo {
    case NULL =>
      null.asInstanceOf[H]
    case list: ValueList if list.children.size == n() =>
      lazily(factory)(list.children.iterator)
  }
}

object ConvertToValue extends Poly1 {
  implicit def caseFrom[T, V <: Value](
    implicit from: Lazy[From.Aux[T, V]]
  ): Case.Aux[T, Value] = at(lazily(from)(_))
}

trait RecordFactory[H <: HList] {
  type Out = H

  def apply(dict: ValueDict): Out
}

object RecordFactory {
  implicit def nilToCaseClass: RecordFactory[HNil] = new RecordFactory[HNil] {
    override def apply(dict: ValueDict): Out = HNil
  }

  implicit def consToCaseClass[K, V, T <: HList](
    implicit
    kv: Witness.Aux[K],
    to: Lazy[To[V]],
    tail: Lazy[RecordFactory[T]]
  ): RecordFactory[FieldType[K, V] :: T] = new RecordFactory[FieldType[K, V] :: T] {
    def apply(dict: ValueDict): Out = {
      val name = kv.value.asInstanceOf[Symbol].name
      labelled.field[K](lazily(to)(dict.at(name).getOrElse(NULL))) :: lazily(tail)(dict)
    }
  }
}

trait HListFactory[H <: HList] {
  type Out = H

  def apply(iterator: Iterator[Value]): Out
}

object HListFactory {
  implicit def nilToValues: HListFactory[HNil] = new HListFactory[HNil] {
    override def apply(iterator: Iterator[Value]): Out = {
      require(iterator.isEmpty)
      HNil
    }
  }

  implicit def consToValues[H, T <: HList](
    implicit
    to: Lazy[To[H]],
    tail: Lazy[HListFactory[T]]
  ): HListFactory[H :: T] = new HListFactory[H :: T] {
    override def apply(iterator: Iterator[Value]): Out = {
      require(iterator.nonEmpty)
      val next = iterator.next()
      lazily(to)(next) :: lazily(tail)(iterator)
    }
  }
}


