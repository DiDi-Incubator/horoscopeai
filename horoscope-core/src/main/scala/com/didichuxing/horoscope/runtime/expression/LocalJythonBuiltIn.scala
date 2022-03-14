package com.didichuxing.horoscope.runtime.expression

import java.io.File

import akka.http.scaladsl.server.Directives.{complete, formFieldMap, parameterMap, path}
import akka.http.scaladsl.server.Route
import com.didichuxing.horoscope.core.FileStore
import com.didichuxing.horoscope.runtime.Implicits.gson
import com.didichuxing.horoscope.runtime.{Value, ValueDict}
import com.didichuxing.horoscope.runtime.expression.BuiltIn.{FuncImpl, MethodImpl}
import com.didichuxing.horoscope.util.Logging
import org.python.core.{PyFunction, PyList, PyObject}
import org.python.util.PythonInterpreter

import scala.collection.JavaConversions._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Try

class LocalJythonBuiltIn(fileStore: FileStore, url: String = "") extends BuiltIn with Logging {

  private val pyInterps = TrieMap.empty[String, PythonInterpreter]

  private val pyInterp = PythonInterpreter.threadLocalStateInterpreter(null)

  private val (prefixPath: String, funcFileList: Seq[File], methodFileList: Seq[File]) = loadFileList(url)

  private var functionCache = loadFunctionList()

  private var methodCache = loadMethodList()

  def loadFileList(url: String): (String, Seq[File], Seq[File]) = {
    val (prefixPath, files) = fileStore.listFiles(url)
    val functions = files.filter(_.getAbsolutePath.contains("functions"))
    val methods = files.filter(_.getAbsolutePath.contains("methods"))
    (prefixPath, functions, methods)
  }

  def loadFunctionList(): Map[String, FuncImpl] = {
    funcFileList.flatMap { file => {
      try {
        val source = Source.fromFile(file, "UTF-8")
        val (namespace, name) = path2UDFName(file.getAbsolutePath)
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

  override def path2UDFName(path: String): (String, String) = {
    val prefix = if (prefixPath.takeRight(1) == "/") {
      prefixPath.dropRight(1)
    } else {
      prefixPath
    }
    val namePath = path.replaceAll(s"(${prefix}|\\/functions|\\/methods|.py)", "")
    val pos = namePath.lastIndexOf("/")
    (s"${namePath.take(pos + 1)}", namePath.drop(pos + 1))
  }

  def loadMethodList(): Map[String, MethodImpl] = {
    methodFileList.flatMap { file => {
      try {
        val source = Source.fromFile(file, "UTF-8")
        val (namespace, name) = path2UDFName(file.getAbsolutePath)
        val pyFunc = compile(name, source.mkString, namespace)
        val udfPath = s"$namespace$name"
        info(("msg", s"load python method def $udfPath"))
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

  override def functions: Map[String, FuncImpl] = functionCache

  override def methods: Map[String, MethodImpl] = methodCache

  def mergeFrom(builtIn: BuiltIn): this.type = {
    builtIn.functions.foreach(e => functionCache += (e._1 -> e._2))
    builtIn.methods.foreach(e => methodCache += (e._1 -> e._2))
    this
  }

  override def api: Route = {
    import akka.http.scaladsl.server.Directives._
    import com.didichuxing.horoscope.runtime.Implicits._
    concat(
      path("evaluate") {
        (formFieldMap | parameterMap) { params =>
          val namespace = Try(params("namespace")).getOrElse("/")
          val expression = Try(params("expression")).getOrElse("")
          val context = gson.fromJson(Try(params("context")).getOrElse("{}"), classOf[Value])
          context match {
            case _: ValueDict =>
              val result = Expression(namespace, expression)(this).apply(context.as[ValueDict])
              complete(result)
            case _ => {
              val result = Expression(namespace, expression)(this).apply(Value(Map.empty[String, String]))
              complete(result)
            }
          }
        }
      }
    )
  }
}

object LocalJythonBuiltIn {
  val interpreter: PythonInterpreter = PythonInterpreter.threadLocalStateInterpreter(null)
  def checkJythonSyntax(content: String): Unit = {
    interpreter.exec(content)
  }
}
