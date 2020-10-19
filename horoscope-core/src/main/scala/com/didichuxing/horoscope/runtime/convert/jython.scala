package com.didichuxing.horoscope.runtime.convert

import com.didichuxing.horoscope.runtime._
import org.python.core.{PyObject, _}

import scala.collection.JavaConversions._

trait JythonConvertible {

  implicit def fromPyObject[T <: PyObject]: Value.From.Aux[T, Value] = new Value.From[T] {
    override type ValueType = Value

    override def apply(elem: T): Value = {
      JythonConvertible.pyObject2Value(elem)
    }
  }

  implicit def toPyObject: Value.To[PyObject] = new Value.To[PyObject] {
    override def apply(elem: Value): PyObject = {
      JythonConvertible.value2PyObject(elem)
    }
  }

}

object JythonConvertible {
  def pyObject2Value(p: PyObject): Value = {
    p match {
      case _: PyNone =>
        NULL
      case v: PyString =>
        Text(v.getString)
      case v: PyBoolean =>
        BooleanValue(v.getBooleanValue)
      case v: PyInteger =>
        NumberValue(v.getValue)
      case v: PyLong =>
        NumberValue(v.getValue.longValue())
      case v: PyFloat =>
        NumberValue(v.getValue)
      case v: PySequence =>
        new PyIterableWrapper(v.asIterable())
      case v: PySet =>
        new PyIterableWrapper(v.asIterable())
      case v: PyDictionary =>
        new PyDictWrapper(v)
    }
  }


  def value2PyObject(v: Value): PyObject = {
    v match {
      case NULL =>
        Py.None
      case Text(v) =>
        new PyString(v)
      case BooleanValue(v) =>
        new PyBoolean(v)
      case NumberValue(v) if v.isValidInt || v.isValidShort =>
        new PyInteger(v.intValue())
      case NumberValue(v) if v.isValidLong =>
        new PyLong(v.longValue())
      case NumberValue(v) if v.isDecimalFloat =>
        new PyFloat(v.floatValue())
      case NumberValue(v) if v.isDecimalDouble =>
        new PyFloat(v.doubleValue())
      case v: ValueList =>
        val pyList = new PyList()
        v.iterator.foreach { case (i, v) => pyList.add(i, value2PyObject(v)) }
        pyList
      case v: ValueDict =>
        val pyDict = new PyDictionary()
        v.iterator.foreach { case (k, v) => pyDict.put(k, value2PyObject(v)) }
        pyDict
    }
  }
}

class PyIterableWrapper(it: Iterable[PyObject]) extends ValueList {
  override def children: Seq[Value] = {
    it.toList.map(Value(_))
  }
}

class PyDictWrapper(pyDict: PyDictionary) extends ValueDict {
  override def iterator: Iterator[(String, Value)] = {
    pyDict.getMap.iterator.map { case (k, v) => (k.toString, Value(v)) }
  }
}
