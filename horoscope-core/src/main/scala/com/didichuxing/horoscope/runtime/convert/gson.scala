/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.runtime.convert

import com.didichuxing.horoscope.runtime._
import com.google.gson.internal.Streams
import com.google.gson.stream.{JsonReader, JsonWriter}
import com.google.gson._
import org.apache.commons.codec.binary.Base64

trait DefaultGson {
  implicit val gson: Gson = {
    new GsonBuilder()
      .registerTypeHierarchyAdapter(classOf[Value], new ValueTypeAdapter)
      .create()
  }
}

class ValueTypeAdapter extends TypeAdapter[Value] {
  def write(out: JsonWriter, value: Value): Unit = {
    value match {
      case NULL =>
        out.nullValue()
      case list: ValueList =>
        out.beginArray()
        for (elem <- list.children) {
          write(out, elem)
        }
        out.endArray()
      case dict: ValueDict =>
        out.beginObject()
        for ((name, elem) <- dict) {
          out.name(name)
          write(out, elem)
        }
        out.endObject()
      case BooleanValue(primitive) =>
        out.value(primitive)
      case NumberValue(primitive) =>
        out.value(primitive)
      case Text(text) =>
        out.value(text)
      case Binary(binary) =>
        out.value(Base64.encodeBase64String(binary))
    }
  }

  def read(in: JsonReader): Value = {
    Value(Streams.parse(in))
  }
}

trait GsonConvertible {
  implicit val fromGson: Value.From.Aux[JsonElement, Value] = new GsonConverter
}

class GsonConverter extends Value.From[JsonElement] {
  override type ValueType = Value

  def apply(elem: JsonElement): Value = {
    import com.didichuxing.horoscope.runtime._

    elem match {
      case _: JsonNull =>
        NULL

      case primitive: JsonPrimitive =>
        if (primitive.isBoolean) {
          BooleanValue(primitive.getAsBoolean)
        } else if (primitive.isString) {
          Text(primitive.getAsString)
        } else {
          assert(primitive.isNumber)
          NumberValue(primitive.getAsBigDecimal)
        }

      case array: JsonArray =>
        new JsonArrayWrapper(array)

      case obj: JsonObject =>
        new JsonObjectWrapper(obj)
    }
  }
}

class JsonArrayWrapper(array: JsonArray) extends ValueList {
  import JsonArrayWrapper._

  override val children: Seq[Value] = new JsonSeq(array).view.map(Value(_))
}

object JsonArrayWrapper {
  import scala.collection.JavaConversions._

  class JsonSeq(val array: JsonArray) extends Seq[JsonElement] {
    def length: Int = array.size()

    def apply(idx: Int): JsonElement = array.get(idx)

    def iterator: Iterator[JsonElement] = array.iterator()
  }
}

trait GsonObjectConverter extends Value.From[JsonObject] {
  override type ValueType = ValueDict

  override def apply(elem: JsonObject): ValueDict = new JsonObjectWrapper(elem)
}

class JsonObjectWrapper(obj: JsonObject) extends ValueDict {
  import scala.collection.JavaConversions._

  def iterator: Iterator[(String, Value)] = obj.entrySet().iterator().map(
    entry => (entry.getKey, Value(entry.getValue))
  )
}
