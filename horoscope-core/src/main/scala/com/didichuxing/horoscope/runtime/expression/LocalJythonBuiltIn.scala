package com.didichuxing.horoscope.runtime.expression

import java.io.File

import com.didichuxing.horoscope.runtime.Value
import com.didichuxing.horoscope.runtime.expression.BuiltIn.{FuncImpl, MethodImpl}
import com.didichuxing.horoscope.util.Logging
import org.python.core.{PyFunction, PyList, PyObject}
import org.python.util.PythonInterpreter

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import scala.collection.concurrent.TrieMap
import scala.io.Source

class LocalJythonBuiltIn extends BuiltIn with Logging {

  private val pyInterps = TrieMap.empty[String, PythonInterpreter]

  private val pyInterp = PythonInterpreter.threadLocalStateInterpreter(null)

  private val funcFileList: Seq[File] = loadFileList("functions")

  private val methodFileList: Seq[File] = loadFileList("methods")

  private var functionCache = loadFunctionList()

  private var methodCache = loadMethodList()

  def loadFileList(category: String): Seq[File] = {
    val ccl = Thread.currentThread.getContextClassLoader
    val urls = ccl.getResources("python/" + category).toList
    val roots = urls.map(url => new File(url.getFile))
    roots.flatMap(udfFiles(_))
  }

  /**
   * 遍历子目录
   */
  private def udfFiles(f: File): Seq[File] = {
    val files = ListBuffer[File]()
    f.listFiles().foreach(child => {
      if (child.isDirectory()) {
        files.appendAll(udfFiles(child))
      } else if (child.isFile()) {
        files.append(child)
      }
    })
    files.toList
  }

  def loadFunctionList(): Map[String, FuncImpl] = {
    funcFileList.flatMap { file => {
      try {
        val source = Source.fromFile(file, "UTF-8")
        val (namespace, name) = path2UDFName(file.getPath.replace(".py", ""))
        val pyFunc = compile(name, source.mkString, namespace)
        val udfPath = s"$namespace$name"
        info(("msg", s"load python function def $udfPath"))
        Some(udfPath -> funcProxy(pyFunc))
      } catch {
        case e: Throwable =>
          logError(("python func", s"${file.getAbsolutePath}"), ("ex", e.toString))
          None
      }
    }
    }.toMap
  }

  def funcProxy(func: PyFunction): FuncImpl = {
    args => {
      val pyargs = args.as[PyObject]
      if (pyargs.isInstanceOf[PyList]) {
        Value(func.__call__(pyargs.asInstanceOf[PyList].getArray))
      } else {
        Value(func.__call__(pyargs))
      }
    }
  }

  def loadMethodList(): Map[String, MethodImpl] = {
    methodFileList.flatMap { file => {
      try {
        val source = Source.fromFile(file, "UTF-8")
        val (namespace, name) = path2UDFName(file.getPath.replace(".py", ""))
        val pyFunc = compile(name, source.mkString, namespace)
        val udfPath = s"$namespace$name"
        info(("msg", s"load python function def $udfPath"))
        Some(udfPath -> methodProxy(pyFunc))
      } catch {
        case e: Throwable =>
          logError(("python func", s"${file.getAbsolutePath}"), ("ex", e.toString))
          None
      }
    }
    }.toMap
  }

  def methodProxy(func: PyFunction): MethodImpl = {
    (value, args) => {
      val pyValue = value.as[PyObject]
      val pyArgs = args.as[PyObject]
      val allArgs = new PyList()
      if (pyArgs.isInstanceOf[PyList]) {
        allArgs.add(pyValue)
        pyArgs.asInstanceOf[PyList].asIterable().foreach(v => allArgs.add(v))
      } else {
        allArgs.add(pyValue)
        allArgs.add(pyArgs)
      }
      Value(func.__call__(allArgs.getArray))
    }
  }

  private def compile(name: String, content: String, namespace: String = ""): PyFunction = {
    namespace match {
      case "" =>
        pyInterp.exec(content)
        pyInterp.get(name).asInstanceOf[PyFunction]
      case _: String =>
        val pyInterp = pyInterps.getOrElseUpdate(namespace, PythonInterpreter.threadLocalStateInterpreter(null))
        pyInterp.exec(content)
        pyInterp.get(name).asInstanceOf[PyFunction]
    }
  }

  override def functions: collection.Map[String, FuncImpl] = functionCache

  override def methods: collection.Map[String, MethodImpl] = methodCache

  def mergeFrom(builtIn: BuiltIn): this.type = {
    builtIn.functions.foreach(e => functionCache += (e._1 -> e._2))
    builtIn.methods.foreach(e => methodCache += (e._1 -> e._2))
    this
  }
}

