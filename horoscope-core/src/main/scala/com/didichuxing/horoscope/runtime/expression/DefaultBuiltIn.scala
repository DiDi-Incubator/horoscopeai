package com.didichuxing.horoscope.runtime.expression

import com.didichuxing.horoscope.runtime._

import scala.math.BigDecimal.RoundingMode
import scala.util.Try
import Implicits.gson

trait DefaultBuiltIn {

  import DefaultBuiltIn._

  implicit val builtin: BuiltIn = defaultBuiltin
}

object DefaultBuiltIn {
  val defaultBuiltin: BuiltIn = new BuiltIn.Builder()
    .addFunction("round")(round _)
    .addFunction("ceil")(ceil _)
    .addFunction("floor")(floor _)
    .addFunction("substr")(substr _)
    .addMethod("length")(length)
    .addMethod("max")(max)
    .addMethod("min")(min)
    .addMethod("sum")(sum)
    .addMethod("subsetof")(subsetOf)
    .addMethod("anyof")(anyOf)
    .addMethod("noneof")(noneOf)
    .addFunction("parse_json")(parseJson _)
    .addFunction("split")(split _)
    .addFunction("mk_string")(mkString _)
    .addMethod("distinct")(distinct)
    .addMethod("to_long")(toLong)
    .addMethod("to_string")(toString)
    .addMethod("to_double")(toDouble)
    .addMethod("flatten")(flatten)
    .addMethod("zip")(zip)
    .addMethod("sort_by_key")(sortByKey)
    .addMethod("split_by")(splitBy)
    .addMethod("to_json")(toJson)
    .addFunction("merge_dict")(mergeDict _)
    .addMethod("delete")(delete)
    .addMethod("get_or_else")(getOrElse)
    .addMethod("at_or_else")(atOrElse)
    .addFunction("concat")(concat _)
    .build()

  def round(d: BigDecimal): BigDecimal = {
    d.setScale(0, RoundingMode.HALF_UP)
  }

  def ceil(d: BigDecimal): BigDecimal = {
    d.setScale(0, RoundingMode.CEILING)
  }

  def floor(d: BigDecimal): BigDecimal = {
    d.setScale(0, RoundingMode.FLOOR)
  }

  def substr(s: Text, from: Int, to: Int): Text = {
    Text(s.underlying.substring(from, to))
  }

  def length(value: Value)(): Int = {
    value match {
      case doc: Document => doc.children.size
      case text: Text => text.length
      case _ => throw new IllegalArgumentException(s"type ${value.valueType} does not have length method")
    }
  }

  def max(list: ValueList)(): Value = {
    list.elementType match {
      case Some("number") =>
        list.children.asInstanceOf[Seq[NumberValue]].max
      case Some("string") =>
        list.children.asInstanceOf[Seq[Text]].max
      case _ =>
        NULL
    }
  }

  def min(list: ValueList)(): Value = {
    list.elementType match {
      case Some("number") =>
        list.children.asInstanceOf[Seq[NumberValue]].min
      case Some("string") =>
        list.children.asInstanceOf[Seq[Text]].min
      case _ =>
        NULL
    }
  }

  def sum(list: ValueList)(): Value = {
    list.elementType match {
      case Some("number") =>
        list.children.asInstanceOf[Seq[NumberValue]].sum
      case _ =>
        NULL
    }
  }

  def subsetOf(values: Seq[Value])(those: Set[Value]): Boolean = {
    values.forall(those.contains)
  }

  def anyOf(values: Seq[Value])(those: Set[Value]): Boolean = {
    values.exists(those.contains)
  }

  def noneOf(values: Seq[Value])(those: Set[Value]): Boolean = {
    values.forall(!those.contains(_))
  }

  def zip(value: Seq[ValueDict])(those: Seq[ValueDict]): Seq[ValueDict] = {
    if (value.size != those.size) {
      throw new IllegalArgumentException(s"zip participants don't have the same size")
    } else {
      value.zip(those).map { case (left, right) =>
        new SimpleDict(left.as[Map[String, Value]] ++ right.as[Map[String, Value]])
      }
    }
  }

  def sortByKey(value: Seq[ValueDict])(key: String): Seq[ValueDict] = {
    value.sortBy { dict =>
      dict.visit(key) match {
        case NumberValue(v) => v.toLong
        case _ => throw new NoSuchMethodException(s"type ${dict.visit(key).valueType} does not used as sort key")
      }
    }
  }

  def toLong(value: Value)(): NumberValue = {
    value match {
      case text: Text => Try(NumberValue(text.underlying.toLong)).getOrElse(
        throw new IllegalArgumentException(s"string value: ${text.underlying} cannot be parsed to long"))
      case NULL => NumberValue(0)
      case NumberValue(underlying) => NumberValue(underlying.longValue())
      case _ => throw new NoSuchMethodException(s"type ${value.valueType} does not have to_long method")
    }
  }

  def toString(value: Value)(): Text = {
    value match {
      case number: NumberValue => Text(number.toString)
      case NULL => Text("null")
      case t: Text => t
      case BooleanValue(b) => Text(b.toString)
      case Binary(b) => Text(b.toString)
      case _ => throw new NoSuchMethodException(s"type ${value.valueType} does not have to_string method")
    }
  }

  def parseJson(s: String): ValueDict = {
    Implicits.gson.fromJson(s, classOf[ValueDict])
  }

  def split(s: String, regex: String): Array[String] = {
    s.split(regex)
  }

  def splitBy(s: String)(regex: String): Array[String] = s.split(regex)

  def mkString(input: Array[String], sep: String): String = {
    input.mkString(sep)
  }

  def distinct(list: ValueList)(): ValueList = {
    new SimpleList(list.children.distinct)
  }

  def flatten(value: Value)(): Value = {
    value match {
      case list: ValueList =>
        Value(list.children.flatMap(_.as[ValueList].children))
      case _ => throw new NoSuchMethodException(s"type ${value.valueType} does not have flatten method")

    }
  }

  def toDouble(v: Value)(): Double = {
    v match {
      case NumberValue(underlying) =>
        underlying.doubleValue()
      case Text(underlying) =>
        underlying.toDouble
      case NULL =>
        0.0
      case _ =>
        throw new NumberFormatException(s"Unsupported value type $v for toDouble")
    }
  }

  def toJson(value: ValueDict)(): String = {
    value.toJson
  }

  //dict的深度merge，用于整合结构相似的dict
  def mergeDict(source: Value, target: Value): Value = {
    if (!source.isInstanceOf[ValueDict] && !target.isInstanceOf[ValueDict]) {
      NULL
    } else if (!source.isInstanceOf[ValueDict] && target.isInstanceOf[ValueDict]) {
      target.as[ValueDict]
    } else if (source.isInstanceOf[ValueDict] && !target.isInstanceOf[ValueDict]) {
      source.as[ValueDict]
    } else {
      var merged = target.as[ValueDict]
      source.as[ValueDict].iterator.foreach(v => {
        val sourceKey = v._1
        val sourceValue = v._2
        if (merged.at(sourceKey).isEmpty) {
          //target没有key，增加
          merged = merged.updated(sourceKey, sourceValue)
        } else {
          //target有key
          val targetValue = merged.visit(sourceKey)
          if (sourceValue.isInstanceOf[ValueDict] && targetValue.isInstanceOf[ValueDict]) {
            //如果key是dict
            val tmp = mergeDict(sourceValue.as[ValueDict], targetValue.as[ValueDict])
            merged = merged.updated(sourceKey, tmp)
          } else {
            //如果key不是dict，直接覆盖
            merged = merged.updated(sourceKey, sourceValue)
          }
        }
      })
      merged
    }
  }

  /**
   * 按照keys删除dict中的元素
   */
  def delete(dict: ValueDict)(keys: Array[String]): ValueDict = {
    val iterators = dict.iterator.filterNot(p => keys.contains(p._1)).toArray
    new SimpleDict(iterators.toIterator)
  }

  def getOrElse(v: Value)(newValue: Value): Value = {
    v match {
      case NULL => newValue
      case _ => v
    }
  }

  def atOrElse(value: Value)(index: Value, defaultValue: Value): Value = {
    value match {
      case list: ValueList =>
        list.at(
          Try(index.as[Int]).getOrElse(throw new IllegalArgumentException(
            s"list should use integer index, but ${index.valueType} is given"
          ))
        ).getOrElse(defaultValue)
      case dict: ValueDict =>
        dict.at(
          Try(index.as[String]).getOrElse(throw new IllegalArgumentException(
            s"dict should use string index, but ${index.valueType} is given"
          ))
        ).getOrElse(defaultValue)
      case _ =>
        defaultValue
    }
  }

  def concat(left: Value, right: Value): Value = {
    if (!left.isInstanceOf[ValueList] && !right.isInstanceOf[ValueList]) {
      NULL
    } else if (!left.isInstanceOf[ValueList]) {
      right.as[ValueList]
    } else if (!right.isInstanceOf[ValueList]) {
      left.as[ValueList]
    } else {
      Value(List.concat(left.as[ValueList].children, right.as[ValueList].children))
    }
  }
}
