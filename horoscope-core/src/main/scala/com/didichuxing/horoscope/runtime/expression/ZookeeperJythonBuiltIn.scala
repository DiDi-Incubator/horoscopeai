package com.didichuxing.horoscope.runtime.expression

import java.util.concurrent.{Executors, Semaphore}

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import com.didichuxing.horoscope.runtime.{Value, ValueDict}
import com.didichuxing.horoscope.runtime.expression.BuiltIn.{FuncImpl, MethodImpl}
import com.didichuxing.horoscope.util.Logging
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{TreeCache, TreeCacheEvent, TreeCacheListener}
import org.python.core.{PyFunction, PyList, PyObject}
import org.python.util.PythonInterpreter

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.util.Try

class ZookeeperJythonBuiltIn(curator: CuratorFramework) extends TreeCacheListener with BuiltIn with Logging {

  import ZookeeperJythonBuiltIn._

  import scala.collection.JavaConversions._

  private val executor = Executors.newSingleThreadExecutor()

  implicit def executionContext: ExecutionContext = ExecutionContext.fromExecutor(executor)

  private val isInitialized: Semaphore = new Semaphore(0)

  private val pyInterps = TrieMap.empty[String, PythonInterpreter]

  private val pyInterp = PythonInterpreter.threadLocalStateInterpreter(null)

  private val functionCache = TrieMap.empty[String, FuncImpl]

  private val methodCache = TrieMap.empty[String, MethodImpl]

  private val pyFunctionCache = TrieMap.empty[String, PyFunction]

  private val pyMethodProxyCache = TrieMap.empty[String, PyFunction]

  private val tree: TreeCache = {
    val cache = TreeCache.newBuilder(curator.usingNamespace(curator.getNamespace + "/tree"), "/")
      .setCreateParentNodes(true)
      .setExecutor(executor)
      .build()
    cache.getListenable.addListener(this)
    cache.start()

    cache
  }

  isInitialized.acquire()

  protected def childEvent(client: CuratorFramework, event: TreeCacheEvent): Unit = synchronized {
    import TreeCacheEvent.Type._

    if (event.getType == INITIALIZED) {
      isInitialized.release()
    }

    if (event.getType == NODE_ADDED || event.getType == NODE_UPDATED) {
      val data = event.getData
      if (data.getData != null && data.getData.nonEmpty) {
        val (namespace, name) = path2UDFName(data.getPath)
        val func = compile(name, new String(data.getData), namespace)
        val udfPath = s"$namespace$name"
        if (data.getPath.contains("functions")) {
          pyFunctionCache.update(udfPath, func)
          functionCache.update(udfPath, funcProxy(udfPath))
        } else if (data.getPath.contains("methods")) {
          pyMethodProxyCache.update(udfPath, func)
          methodCache.update(udfPath, methodProxy(udfPath))
        }
      }
    }

    if (event.getType == NODE_REMOVED) {
      val data = event.getData
      val (namespace, name) = path2UDFName(data.getPath)
      val udfPath = s"$namespace$name"
      if (data.getPath.contains("functions")) {
        pyFunctionCache.remove(udfPath)
        functionCache.remove(udfPath)
      } else if (data.getPath.contains("methods")) {
        pyMethodProxyCache.remove(udfPath)
        methodCache.remove(udfPath)
      }
    }
  }

  def funcProxy(name: String): FuncImpl = {
    args => {
      val func = pyFunctionCache.get(name)
      if (func.isEmpty) {
        throw new NotImplementedError(s"no $name defined in builtin")
      } else {
        val pyargs = args.as[PyObject]
        if (pyargs.isInstanceOf[PyList]) {
          Value(func.get.__call__(pyargs.asInstanceOf[PyList].getArray))
        } else {
          Value(func.get.__call__(pyargs))
        }
      }
    }
  }

  def methodProxy(name: String): MethodImpl = {
    (value, args) => {
      val func = pyMethodProxyCache.get(name)
      if(func.isEmpty) {
        throw new NotImplementedError(s"no $name defined in builtin")
      } else {
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
        Value(func.get.__call__(allArgs.getArray))
      }
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

  private def update(text: String, remaining: Option[String] = None): Unit = {
    val (ns, name) = path2UDFName(remaining.get)
    val func = compile(name, text, ns)
    require(func != null && name == func.getFuncName.getString)

    val path = s"/tree${remaining.get}"
    curator.createContainers(path)
    curator.setData().forPath(path, text.getBytes)
  }

  def mergeFrom(builtIn: BuiltIn): this.type = {
    builtIn.functions.foreach(e => functionCache.update(e._1, e._2))
    builtIn.methods.foreach(e => methodCache.update(e._1, e._2))
    this
  }

  override def functions: scala.collection.Map[String, FuncImpl] = functionCache

  override def methods: scala.collection.Map[String, MethodImpl] = methodCache

  //scalastyle:off
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
      },
      path("tree" ~ Remaining) {
        case "" =>
          //返回树形结构
          complete(Value(walk()))
        case remaining: String if remaining.endsWith("/") =>
          //返回子目录列表
          val path = if (remaining == "/") remaining else remaining.init
          complete(Value(listChildren(path)))
        case remaining: String =>
          // flow operations
          concat(
            get {
              //获取func内容
              complete(new String(tree.getCurrentData(remaining).getData))
            },
            put {
              //更新func内容
              entity(as[String]) { value =>
                complete(Try {
                  if (remaining.startsWith("/functions") | remaining.startsWith("/methods")) {
                    update(value, Some(remaining))
                    StatusCodes.OK
                  } else {
                    throw new IllegalArgumentException("udf path must start with /functions or /methods")
                  }
                })
              }
            },
            delete {
              //删除func内容
              complete(Try {
                curator.delete().deletingChildrenIfNeeded().forPath("/tree" + remaining)
                StatusCodes.OK
              })
            }
          )
      }
    )
  }

  def walk(segments: Seq[String] = Nil): Node = {
    val path = segments.mkString("/", "/", "")
    val data = tree.getCurrentData(path)
    val flowName: String = if (data != null && data.getData != null && data.getData.nonEmpty) {
      path
    } else {
      null
    }

    val children = tree.getCurrentChildren(path).keysIterator.map(
      child => walk(segments :+ child)
    ).toArray

    Node(segments.lastOption.getOrElse("/"), flowName, children)
  }

  def listChildren(path: String): Seq[String] = {
    tree.getCurrentChildren(path).toList.flatMap({ case (key, value) =>
      val fullPath = if (path == "/") s"/$key" else s"$path/$key"
      var items: List[String] = Nil

      if (tree.getCurrentChildren(fullPath) != null && tree.getCurrentChildren(fullPath).nonEmpty) {
        items ::= fullPath + "/"
      }
      if (value.getData != null && value.getData.nonEmpty) {
        items ::= fullPath
      }
      items
    })
  }
}

object ZookeeperJythonBuiltIn {

  case class Node(name: String, flow: String, children: Seq[Node])

}
