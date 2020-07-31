/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description: define JSON like data model, and JSONPath like visitor methods
 */

package com.didichuxing.horoscope.runtime

import java.io.StringWriter
import java.util.concurrent.atomic.AtomicLong

import com.google.gson.stream.JsonWriter

import scala.collection.immutable.{ListMap, Queue, Stack}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions

object Value extends convert.Implicits with ops.Implicits {
  trait From[T] {
    type ValueType <: Value

    def apply(elem: T): ValueType
  }

  object From {
    // scalastyle:off
    type Aux[T, V <: Value] = From[T] { type ValueType = V }

    def apply[T](implicit inst: From[T]): Aux[T, inst.ValueType] = inst
  }

  trait To[T] {
    def apply(elem: Value): T
  }

  object To {
    def apply[T](implicit inst: To[T]): To[T] = inst
  }

  def apply[T, V <: Value](elem: T)(
    implicit converter: From.Aux[T, V]
  ): V = converter(elem)
}

sealed trait Value extends Any

case object NULL extends Value {
  override def toString: String = "null"
}

sealed trait PrimitiveValue[T] extends Any with Value {
  def underlying: T

  override def toString: String = underlying.toString
}

case class BooleanValue(underlying: Boolean) extends AnyVal with PrimitiveValue[Boolean]

case class NumberValue(underlying: BigDecimal) extends AnyVal with PrimitiveValue[BigDecimal]

case class Text(underlying: String) extends AnyVal with PrimitiveValue[String] {
  override def toString: String = {
    val content = new StringWriter()
    val writer = new JsonWriter(content)
    writer.value(underlying)
    content.toString
  }
}

case class Binary(underlying: Array[Byte]) extends AnyVal with PrimitiveValue[Array[Byte]] {
  override def toString: String = s"blob[${underlying.length}]"
}

// TODO(wenxiang): support TypedDocument
sealed trait Document extends Value {
  import Document._

  type IndexType

  // [expr <- doc]
  def transform(func: Processor): Document

  // doc[i] or doc[key]
  def at(index: IndexType): Option[Value]

  // doc.name
  def visit(name: String): Value

  // doc.*
  def visit(wildcard: Wildcard): Value

  // doc..name
  def search(name: String): Value = {
    val builder = Seq.newBuilder[Value]
    var stack: List[Document] = Nil

    stack = this :: stack
    while (stack.nonEmpty) {
      val top :: rest = stack
      top match {
        case dict: ValueDict =>
          dict.at(name).foreach({
            case list: ValueList =>
              builder ++= list.children
            case value: Value =>
              builder += value
          })
        case _ =>
      }
      stack = top.children.foldRight(rest)((child, rest) =>
        child match {
          case doc: Document => doc :: rest
          case _ => rest
        }
      )
    }

    val results = builder.result()
    if (results.isEmpty) {
      NULL
    } else {
      new SimpleList(results)
    }
  }

   // doc..*
   def search(wildcard: Wildcard): Value = {
     val builder = Seq.newBuilder[Value]

     var next: Value = visit(*)
     while (next != NULL) {
       next match {
         case list: ValueList =>
           builder ++= list.children
           next = list.visit(*)
         case _ =>
           next = NULL
       }
     }

     val results = builder.result()
     if (results.isEmpty) {
       NULL
     } else {
       new SimpleList(results)
     }
   }

  def iterator: Iterator[(IndexType, Value)]

  def children: Iterable[Value]

  def table: TableView = TableView(this)
}

object Document {
  type Pair = (Value, Value) // (index, value), presented by # and _
  type Processor = PartialFunction[Pair, Value]
}

object ValueList {
  val empty: ValueList = new SimpleList(Nil)

  @scala.annotation.tailrec
  def unapply(value: Value): Option[Seq[Value]] = value match {
    case list: ValueList => Some(list.children)
    case table: TableView if table.dims == 1 => unapply(table.value)
    case NULL => Some(Nil)
    case _ => None
  }
}

trait ValueList extends Document with Iterable[(Int, Value)] {
  import Document._

  override type IndexType = Int

  override def children: Seq[Value]

  override def iterator: Iterator[(IndexType, Value)] = children.iterator.zipWithIndex.map(_.swap)

  override def transform(func: Processor): ValueList = {
    new SimpleList(
      iterator.map(elem => NumberValue(elem._1) -> elem._2).collect(func).toArray
    )
  }

  override def at(index: Int): Option[Value] = {
    if (index < 0) {
      children.lift(children.size + index)
    } else {
      children.lift(index)
    }
  }

  override def visit(name: String): Value = {
    val results = children.flatMap({
      case dict: ValueDict =>
        dict.at(name) match {
          case Some(list: ValueList) => list.children
          case child: Option[Value] => child
        }
      case _ =>
        Nil
    })

    if (results.isEmpty) {
      NULL
    } else {
      new SimpleList(results)
    }
  }

  override def visit(wildcard: Wildcard): Value = {
    val results = children.flatMap({
      case list: ValueList => list.children
      case dict: ValueDict => ValueList.unapply(dict.visit(*)).toIterable.flatten
      case _ => Nil
    })

    if (results.isEmpty) {
      NULL
    } else {
      new SimpleList(results)
    }
  }

  override final def equals(obj: Any): Boolean = {
    obj match {
      case that: ValueList => children == that.children
      case _ => false
    }
  }

  override def toString: String = {
    children.mkString("[", ", ", "]")
  }
}

class SimpleList(val children: Seq[Value]) extends ValueList {
  def this(iterable: Iterable[Value]) = this(iterable.toSeq)
}

object ValueDict {
  val empty: ValueDict = new SimpleDict(Map.empty[String, Value])

  @scala.annotation.tailrec
  def unapply(value: Value): Option[Map[String, Value]] = value match {
    case dict: SimpleDict => Some(dict.underlying)
    case dict: ValueDict => Some(dict.toMap)
    case table: TableView if table.dims == 1 => unapply(table.value)
    case NULL => Some(Map.empty)
    case _ => None
  }
}

// TODO(wenxiang): support TypedObject
trait ValueDict extends Document with Iterable[(String, Value)] {
  import Document._

  override type IndexType = String

  override def children: Iterable[Value] = view.map(_._2)

  override def transform(func: Processor): ValueDict = {
    new SimpleDict(
      iterator.flatMap{
        case (key, value) => func.lift(Text(key), value).map(key -> _)
      }
    )
  }

  override def at(index: String): Option[Value] = {
    iterator.find(_._1 == index).map(_._2)
  }

  override def visit(name: String): Value = {
    at(name).getOrElse(NULL)
  }

  override def visit(wildcard: Wildcard): Value = {
    val results = children.flatMap({
      case list: ValueList => list.children
      case child: Value => child :: Nil
    })

    if (results.isEmpty) {
      NULL
    } else {
      new SimpleList(results)
    }
  }

  def updated(name: String, value: Value): ValueDict = OverrideDict(name, value)(this)

  override def toString: String = {
    val fields = for ((name, value) <- iterator) yield {
      s"${Text(name)}: $value"
    }
    fields.mkString("{", ", ", "}")
  }

  override final def equals(obj: Any): Boolean = {
    obj match {
      case that: ValueDict => this.sameElements(that)
      case _ => false
    }
  }
}

class SimpleDict(val underlying: Map[String, Value]) extends ValueDict {
  def this(iterator: Iterator[(String, Value)]) = this({
    // use list map to keep order
    val builder = ListMap.newBuilder[String, Value]
    for (pair <- iterator) {
      builder += pair
    }
    builder.result()
  })

  def this(keys: Iterable[String], items: Iterable[Value]) = this(keys.iterator.zip(items.iterator))

  override def children: Iterable[Value] = underlying.values

  override def iterator: Iterator[(String, Value)] = underlying.iterator

  override def at(index: String): Option[Value] = underlying.get(index)
}

case class OverrideDict(name: String, value: Value)(backend: ValueDict) extends ValueDict {
  override def at(index: String): Option[Value] = {
    if (index == name) Some(value) else backend.at(index)
  }

  override def iterator: Iterator[(String, Value)] = {
    Some((name, value)).iterator ++ backend.iterator.filter(_._1 != name)
  }
}

sealed class TableView private (
  val dims: Int,
  private val records: Seq[TableView.Record],
  private val default: Value
) extends Document with Iterable[(Value, Value)] {
  import TableView._

  final lazy val impl: Array[Record] = {
    val array = records.view.map(record => Queue(record._1:_*) -> record._2).toArray
    java.util.Arrays.sort(array, recordOrdering)
    array
  }

  final lazy val value: Value = {
    if (impl.isEmpty) {
      default
    } else if (impl.head._1.isEmpty) {
      impl.head._2
    } else {
      val builder = new DocumentBuilder()
      impl.foreach({
        case (indices, value) => builder.update(indices, value)
      })
      builder.build()
    }
  }

  override def toString: String = value.toString

  override type IndexType = Value

  override def iterator: Iterator[(Value, Value)] = impl.iterator.map(recordToValuePair)

  override def children: Iterable[Value] = impl.view.map(_._2)

  override def transform(func: Document.Processor): TableView = explode {
    case (indices, value) => func.lift(indices -> value)
  }

  override def at(index: Value): Option[Value] = Some {
    explode {
      case (_, dict: ValueDict) => Field.unapply(index).flatMap(dict.at)
      case (_, list: ValueList) => Location.unapply(index).flatMap(list.at)
    }
  }

  override def visit(name: String): TableView = explode {
    case (_, doc: Document) => doc.visit(name).as[Option[Value]]
   }

  override def visit(wildcard: Wildcard): TableView = explode {
    case (_, doc: Document) => doc.visit(*).as[Option[Value]]
  }

  override def search(name: String): TableView = explode {
    case (_, doc: Document) => doc.search(name).as[Option[Value]]
  }

  override def search(wildcard: Wildcard): TableView = explode {
    case (_, doc: Document) => doc.search(*).as[Option[Value]]
  }

  def transpose(): TableView = {
    new TableView(
      dims,
      records.view.map({
        case (indices, value) => indices.reverse -> value
      }),
      default
    )
  }

  def update(those: Seq[Record]): TableView = {
    val merged = this.impl ++ those

    if (merged.isEmpty) {
      new TableView(dims, Nil, default)
    } else {
      java.util.Arrays.sort(merged, recordOrdering)
      val iterator = merged.reverseIterator

      var result: List[Record] = iterator.next :: Nil
      while (iterator.hasNext) {
        val next = iterator.next()
        if (result.head._1 != next._1) {
          result = next :: result
        }
      }

      new TableView(dims, result, default)
    }
  }

  // doc[begin: end]
  def slice(begin: Value, end: Value): TableView = extend {
    case (_, list: ValueList) =>
      lazy val size = list.children.size
      def offset(pos: Int): Int = math.max(0, {
        if (pos >= 0) pos else size + pos
      })
      val range = (begin, end) match {
        case (Location(i), Location(j)) => Some(offset(i) -> offset(j))
        case (Location(i), NULL) => Some(offset(i) -> Int.MaxValue)
        case (NULL, Location(j)) => Some(0 -> offset(j))
        case (NULL, NULL) => Some(0 -> Int.MaxValue)
        case _ => None
      }
      range.toIterable.flatMap({
        case (begin, end) =>
          list.children.slice(begin, end).zipWithIndex.map({
            case (value, i) => Location(begin + i) -> value
          })
      })

    case (_, dict: ValueDict) =>
      val iterator = begin match {
        case Field(name) =>
          dict.iterator.dropWhile(_._1 != name)
        case _ =>
          dict.iterator
      }

      val last = Field.unapply(end)
      var isDone: Boolean = false

      val builder = Iterable.newBuilder[(Index, Value)]
      while (iterator.nonEmpty && !isDone) {
        val (key, value) = iterator.next()
        builder += (Field(key) -> value)
        isDone = last.contains(key)
      }

      builder.result()
  }

  // doc[i0, i1, ...]
  def select(indices: Seq[Value]): TableView = extend {
    case (_, list: ValueList) =>
      indices.flatMap({
        case Location(i) => list.at(i).map(Location(i) -> _)
        case _ => None
      })

    case (_, dict: ValueDict) =>
      indices.flatMap({
        case Field(name) => dict.at(name).map(Field(name) -> _)
        case _ => None
      })
  }

  // doc[*]
  def select(wildcard: Wildcard): TableView = extend {
    case (_, list: ValueList) =>
      list.iterator.map({
        case (i, value) => Location(i) -> value
      }).toIterable
    case (_, dict: ValueDict) =>
      dict.iterator.map({
        case (name, value) => Field(name) -> value
      }).toIterable
  }

  // doc[?(expr)]
  def query(func: Document.Processor): TableView = extend {
    case (indices, list: ValueList) =>
      list.flatMap({
        case (i, value) =>
          if (func(indices.enqueue(Location(i)) -> value).as[Boolean]) {
            (Location(i) -> value) :: Nil
          } else {
            Nil
          }
      })
    case (indices, dict: ValueDict) =>
      dict.flatMap({
        case (name, value) =>
          if (func(indices.enqueue(Field(name)) -> value).as[Boolean]) {
            (Field(name) -> value) :: Nil
          } else {
            Nil
          }
      })
  }

  // doc[+(expr)]
  def expand(func: Document.Processor): TableView = extend {
    case (indices, value) =>
      func.lift(indices -> value).map(Index(_) -> value)
  }

  // doc[/(expr)]
  def project(func: PartialFunction[(Value, Value), Value]): TableView = extend {
    case (indices, list: ValueList) =>
      val groups = list.groupBy({
        case (i, value) =>
          func.lift(indices.enqueue(Location(i)) -> value).map(Index(_)).getOrElse(Anonymous())
      })
      groups.mapValues(children => Value(children.map(_._2)))
    case (indices, dict: ValueDict) =>
      val groups = dict.groupBy({
        case (name, value) =>
          func.lift(indices.enqueue(Field(name)) -> value).map(Index(_)).getOrElse(Anonymous())
      })
      groups.mapValues(Value(_))
  }

  final def explode(fn: PartialFunction[Record, Option[Value]]): TableView = {
    new TableView(
      dims,
      records.view.flatMap(record =>
        fn.applyOrElse(record, (_: Record) => None).map(record._1 -> _)
      ),
      default
    )
  }

  final def extend(fn: PartialFunction[Record, Iterable[(Index, Value)]]): TableView = {
    new TableView(
      dims + 1,
      records.view.flatMap(record =>
        fn.applyOrElse(record, (_: Record) => Nil).map({
          case (index, value) => record._1.enqueue(index) -> value
        })
      ),
      default
    )
  }
}

object TableView {
  import Ordering.Implicits._

  type Indices = Queue[Index]

  type Record = (Indices, Value)

  implicit def recordToValuePair(record: Record): Document.Pair = {
    new SimpleList(record._1.view.map(_.value)) -> record._2
  }

  implicit val recordOrdering: Ordering[Record] = Ordering.by(_._1)

  def apply(doc: Document): TableView = doc match {
    case table: TableView => table
    case list: ValueList => new TableView(0, Seq(Queue.empty -> list), ValueList.empty)
    case dict: ValueDict => new TableView(0, Seq(Queue.empty -> dict), ValueDict.empty)
  }


  final class DocumentBuilder(var child: Option[(Index, ValueBuilder)] = None) extends ValueBuilder {
    val list = new ListBuffer[Value]()
    var dict = Map.empty[String, Value]

    override def update(indices: Indices, value: Value): Unit = {
      val (head, tail) = indices.dequeue

      if (child.exists(_._1 != head)) {
        buildChild()
      }

      child match {
        case Some((_, builder)) =>
          builder.update(tail, value)
        case None =>
          child = ValueBuilder(indices, value)
      }
    }

    def build(): Document = {
      buildChild()

      if (dict.nonEmpty) {
        for ((value, index) <- list.iterator.zipWithIndex) {
          dict += index.toString -> value
        }
        new SimpleDict(dict)
      } else {
        new SimpleList(list.toArray)
      }
    }

    private def buildChild(): Unit = {
      for ((index, builder) <- child) {
        index match {
          case field: Field => dict += field.name -> builder.build()
          case _ => list += builder.build()
        }
      }
      child = None
    }
  }

  sealed trait ValueBuilder {
    def update(indices: Indices, value: Value): Unit = {}

    def build(): Value
  }

  object ValueBuilder {
    final def apply(indices: Indices, value: Value): Option[(Index, ValueBuilder)] = {
      if (indices.isEmpty) {
        None
      } else {
        Some {
          indices.head -> {
            if (indices.tail.isEmpty) {
              new ValueBuilder {
                def build(): Value = value
              }
            } else {
              new DocumentBuilder(apply(indices.tail, value))
            }
          }
        }
      }
    }
  }

  sealed trait Index extends Ordered[Index] {
    def value: Value
  }

  object Index {
    def apply(value: Value): Index = value match {
      case Location(i) => Location(i)
      case Field(name) => Field(name)
      case _ => Anonymous()
    }
  }

  case class Field(name: String) extends Index {
    override def value: Value = Text(name)

    def compare(that: Index): Int = that match {
      case that: Field => Ordering.String.compare(this.name, that.name)
      case _ => 1
    }
  }

  object Field {
    def unapply(value: Value): Option[String] = value match {
      case Text(name) => Some(name)
      case _ => None
    }
  }

  case class Location(pos: Int) extends Index {
    override def value: Value = NumberValue(pos)

    def compare(that: Index): Int = that match {
      case that: Location => Ordering.Int.compare(this.pos, that.pos)
      case _ => -1
    }
  }

  object Location {
    def unapply(value: Value): Option[Int] = value match {
      case number: NumberValue => Some(number.underlying.intValue())
      case _ => None
    }

  }

  case class Anonymous(id: Long) extends Index {
    override def value: Value = NULL

    def compare(that: Index): Int = that match {
      case that: Anonymous => Ordering.Long.compare(this.id, that.id)
      case _: Location => 1
      case _: Field => -1
    }
  }

  object Anonymous {
    private val id = new AtomicLong(0)

    def apply(): Anonymous = new Anonymous(id.getAndIncrement())
  }
}
