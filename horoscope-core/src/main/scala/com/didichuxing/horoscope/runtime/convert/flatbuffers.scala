package com.didichuxing.horoscope.runtime.convert

import com.didichuxing.horoscope.core.fb._
import com.didichuxing.horoscope.runtime._
import com.google.flatbuffers.{ByteVector, FlatBufferBuilder, StringVector}
import com.didichuxing.horoscope.runtime.convert.FlatBufferConverterUtil._

trait LowPriorityFlatBufferConvertible

trait FlatBufferConvertible extends LowPriorityFlatBufferConvertible {
  implicit def fromFlatBuffer: Value.From.Aux[FlowValue, Value] = new FlatBufferConverter
}

class FlatBufferConverter extends Value.From[FlowValue] {
  override type ValueType = Value

  override def apply(elem: FlowValue): Value = {
    elem.catalog() match {
      case Catalog.Binary => Binary(elem.binaryVector().toBytes.toArray)
      case Catalog.Fractional => NumberValue(BigDecimal(elem.fractional()))
      case Catalog.Integral => NumberValue(BigDecimal(elem.integral()))
      case Catalog.Text => Text(elem.text())
      case Catalog.Boolean => BooleanValue(elem.booleanValue())
      case Catalog.List => new ListWrapper(elem.listVector())
      case Catalog.Dict => new DictWrapper(elem.dictVector())
      case Catalog.Null => NULL
    }
  }
}

// scalastyle:off
object FlatBufferConverterUtil {
  implicit class BinaryVectorConverter(vector: ByteVector) {
    def toBytes: Seq[Byte] = {
      val length = vector.length()
      for (i <- Range(0, length)) yield {
        val child = vector.get(i)
        child
      }
    }
  }

  implicit class DictWrapper(vector: Pair.Vector) extends ValueDict {
    override def iterator: Iterator[(String, Value)] = {
      val length = vector.length()
      val result = for (i <- Range(0, length)) yield {
        val child = vector.get(i)
        (child.key(), Value(child.value()))
      }
      result.toIterator
    }
  }

  implicit class ListWrapper(vector: FlowValue.Vector) extends ValueList {
    override def children: Seq[Value] = {
      val length = vector.length()
      for (i <- Range(0, length)) yield {
        val child = vector.get(i)
        Value(child)
      }
    }
  }


  // Write Value into FlatBuffer builder, return offset
  def pack(value: Value, input: FlatBufferBuilder): Int = {
    val builder = input
    value match {
      case NULL =>
        FlowValue.startFlowValue(builder)
        FlowValue.addCatalog(builder, Catalog.Null)
        FlowValue.endFlowValue(builder)
      case Binary(b) =>
        val binaryOffset = builder.createByteVector(b)
        FlowValue.startFlowValue(builder)
        FlowValue.addCatalog(builder, Catalog.Binary)
        FlowValue.addBinary(builder, binaryOffset)
        FlowValue.endFlowValue(builder)
      case Text(t) =>
        val strOft = builder.createString(t)
        FlowValue.startFlowValue(builder)
        FlowValue.addCatalog(builder, Catalog.Text)
        FlowValue.addText(builder, strOft)
        FlowValue.endFlowValue(builder)
      case BooleanValue(b) =>
        FlowValue.startFlowValue(builder)
        FlowValue.addCatalog(builder, Catalog.Boolean)
        FlowValue.addBooleanValue(builder, b)
        FlowValue.endFlowValue(builder)
      case NumberValue(p) =>
        var longVal: Option[Long] = None
        var doubleVal: Option[Double] = None
        try {
          if (p.scale == 0) {
            longVal = Some(p.toLongExact)
          } else {
            doubleVal = Some(p.toDouble)
          }
        } catch {
          case _: ArithmeticException =>
            doubleVal = Some(p.toDouble)
        }
        if (longVal.isDefined) {
          FlowValue.startFlowValue(builder)
          FlowValue.addCatalog(builder, Catalog.Integral)
          FlowValue.addIntegral(builder, longVal.get)
          FlowValue.endFlowValue(builder)
        } else {
          FlowValue.startFlowValue(builder)
          FlowValue.addCatalog(builder, Catalog.Fractional)
          FlowValue.addFractional(builder, doubleVal.get)
          FlowValue.endFlowValue(builder)
        }
      case list: ValueList =>
        val childrenOffsets = list.children.map({ v => pack(v, builder) }).toArray
        val listOffset = FlowValue.createListVector(builder, childrenOffsets)
        FlowValue.startFlowValue(builder)
        FlowValue.addCatalog(builder, Catalog.List)
        FlowValue.addList(builder, listOffset)
        FlowValue.endFlowValue(builder)
      case dict: ValueDict =>
        val pairOffsets = dict.iterator.map { case (key, value) =>
          val keyOffset = builder.createString(key)
          val valueOffset = pack(value, builder)
          Pair.startPair(builder)
          Pair.addKey(builder, keyOffset)
          Pair.addValue(builder, valueOffset)
          Pair.endPair(builder)
        }.toArray
        val dictOffset = FlowValue.createDictVector(builder, pairOffsets)
        FlowValue.startFlowValue(builder)
        FlowValue.addCatalog(builder, Catalog.Dict)
        FlowValue.addDict(builder, dictOffset)
        FlowValue.endFlowValue(builder)
    }
  }
}
