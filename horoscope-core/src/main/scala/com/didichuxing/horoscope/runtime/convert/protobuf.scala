/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.runtime.convert

import com.didichuxing.horoscope.core.FlowRuntimeMessage.{FlowValue, FlowValueOrBuilder}
import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowValue.ValueCase
import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowValue.ValueCase._
import com.didichuxing.horoscope.runtime._
import com.didichuxing.horoscope.runtime.convert.FlowValueConverter.{DictWrapper, ListWrapper}
import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType._
import com.google.protobuf.{ByteString, Descriptors, MapEntry, Message, MessageOrBuilder}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Try

trait LowPriorityProtobufConvertible

trait ProtobufConvertible extends LowPriorityProtobufConvertible {
  implicit def fromMessage[T <: MessageOrBuilder]: Value.From.Aux[T, Value] = new MessageConverter[T]

  implicit def toFlowValue: Value.To[FlowValue] = newTo {
    case table: TableView => FlowValueConverter.encode(table.value)
    case value: Value => FlowValueConverter.encode(value)
  }
}

//any pb -> value
class MessageConverter[T <: MessageOrBuilder] extends Value.From[T] {
  override type ValueType = Value

  def apply(message: T): Value = {
    if (message.isInstanceOf[FlowValue]) {
      getFlowValue(message.asInstanceOf[FlowValue])
    } else {
      val values = mutable.Map[String, Value]()
      message.getAllFields.foreach {
        case (field: FieldDescriptor, data: Any) =>
          val fieldType = field.getJavaType()
          if (field.isMapField) {
            values.put(field.getName, getMapValue(message, field))
          } else if (field.isRepeated) {
            values.put(field.getName, getListValue(message, field))
          } else {
            values.put(field.getName, getSingleValue(data, fieldType))
          }
      }
      new SimpleDict(values.toMap)
    }
  }

  def getMapValue(message: T, field: Descriptors.FieldDescriptor): ValueDict = {
    val values = mutable.Map[String, Value]()
    val count = message.getRepeatedFieldCount(field)
    for (i <- 0 until count) {
      val rf = message.getRepeatedField(field, i)
      if (rf.isInstanceOf[MapEntry[String, Any]]) {
        val mapEntry = rf.asInstanceOf[MapEntry[String, Any]]
        val key = mapEntry.getKey
        val value = mapEntry.getValue
        val msg = getSingleValue(value, field.getJavaType)
        values.put(key, msg)
      }
    }
    new SimpleDict(values.toMap)
  }

  def getListValue(message: T, field: Descriptors.FieldDescriptor): ValueList = {
    val listField = ListBuffer[Value]()
    val count = message.getRepeatedFieldCount(field)
    for (i <- 0 until count) {
      val data = message.getRepeatedField(field, i)
      val msg = getSingleValue(data, field.getJavaType)
      listField.add(msg)
    }
    new SimpleList(listField.toList)
  }

  def getSingleValue(data: Any, jty: Descriptors.FieldDescriptor.JavaType): Value = {
    Try(jty match {
      case INT =>
        Value(data.asInstanceOf[Int])
      case LONG =>
        Value(data.asInstanceOf[Long])
      case FLOAT =>
        Value(data.asInstanceOf[Float])
      case DOUBLE =>
        Value(data.asInstanceOf[Double])
      case JavaType.BOOLEAN =>
        Value(data.asInstanceOf[Boolean])
      case STRING =>
        Value(data.asInstanceOf[String])
      case BYTE_STRING =>
        Binary(data.asInstanceOf[ByteString].toByteArray)
      case MESSAGE =>
        Value(data.asInstanceOf[Message])
      case _ =>
        NULL
    }).getOrElse(NULL)
  }

  def getFlowValue(message: FlowValueOrBuilder): Value = {
    message.getValueCase match {
      case VALUE_NOT_SET => NULL
      case BINARY => Binary(message.getBinary.toByteArray)
      case FRACTIONAL => NumberValue(BigDecimal(message.getFractional))
      case INTEGRAL => NumberValue(BigDecimal(message.getIntegral))
      case TEXT => Text(message.getText)
      case ValueCase.BOOLEAN => BooleanValue(message.getBoolean)
      case LIST => new ListWrapper(message.getList)
      case DICT => new DictWrapper(message.getDict)
    }
  }
}

object FlowValueConverter {
  import scala.collection.JavaConversions._

  class DictWrapper(dict: FlowValue.DictOrBuilder) extends ValueDict {
    override def iterator: Iterator[(String, Value)] = {
      dict.getChildMap.iterator.map(elem => elem._1 -> Value(elem._2))
    }
  }

  class ListWrapper(list: FlowValue.ListOrBuilder) extends ValueList {
    override def children: Seq[Value] = list.getChildList.view.map(Value(_))
  }

  def encode(value: Value): FlowValue = {
    val builder = FlowValue.newBuilder()
    value match {
      case NULL =>
      case list: ValueList =>
        val flowList = FlowValue.List.newBuilder()
        for (v <- list.iterator) {
          flowList.addChild(encode(v._2))
        }
        builder.setList(flowList)
      case dict: ValueDict =>
        val flowDict = FlowValue.Dict.newBuilder()
        for (v <- dict.iterator) {
          flowDict.putChild(v._1, encode(v._2))
        }
        builder.setDict(flowDict)
      case BooleanValue(primitive) =>
        builder.setBoolean(primitive)
      case NumberValue(primitive) =>
        try {
          builder.setIntegral(primitive.toLongExact)
        } catch {
          case _: ArithmeticException =>
            builder.setFractional(primitive.toDouble)
        }
      case Text(text) =>
        if (text != null) {
          builder.setText(text)
        }
      case Binary(binary) =>
        if (binary != null) {
          builder.setBinary(ByteString.copyFrom(binary))
        }
    }
    builder.build()
  }
}
