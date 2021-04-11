package com.didichuxing.horoscope.service.storage

import java.io.File
import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicReference

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import com.didichuxing.horoscope.core.Flow.FlowConf
import com.didichuxing.horoscope.core.FlowDslMessage.{CompositorDef, FlowDef}
import com.didichuxing.horoscope.core._
import com.didichuxing.horoscope.dsl.FlowCompiler
import com.didichuxing.horoscope.runtime.experiment.{ControllerFactory, ExperimentController}
import com.didichuxing.horoscope.runtime.expression.{BuiltIn, DefaultBuiltIn, LocalJythonBuiltInV2}
import com.didichuxing.horoscope.service.storage.GitFlowStore.ResourceStore
import com.didichuxing.horoscope.util.FlowConfParser._
import com.didichuxing.horoscope.util.{FlowGraphBuilder, Logging}
import org.antlr.v4.runtime.CharStreams
import org.apache.commons.io.FileUtils
import spray.json.DefaultJsonProtocol.{StringJsonFormat, seqFormat}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class GitFlowStore(
  configStore: ConfigStore,
  fileStore: FileStore,
  compositorFactories: Map[String, CompositorFactory],
  controllerFactories: Map[String, ControllerFactory],
  extBuiltIn: Option[BuiltIn] = None
) extends FlowStore with ConfigChangeListener with Logging {

  private var flowDefMap: Map[String, FlowDef] = Map.empty
  // flowName -> FlowConf
  private val flowConfCache: TrieMap[String, FlowConf] = TrieMap.empty
  private val flowCache: TrieMap[String, Flow] = TrieMap.empty
  // flowName -> Controller
  private val controllerCache: TrieMap[String, ExperimentController] = TrieMap.empty
  private val builtIn: AtomicReference[BuiltIn] = new AtomicReference[BuiltIn]()
  private val resourceStore: AtomicReference[ResourceStore] = new AtomicReference[ResourceStore]()

  configStore.register(this)
  load(reloadFile = true)

  override implicit def getBuiltIn: BuiltIn = builtIn.get()

  def getResource(namespace: String)(path: String): Array[Byte] = resourceStore.get().getResource(namespace)(path)

  implicit def buildCompositor(flow: String, definition: CompositorDef): Compositor = {
    val factoryName = definition.getFactory
    val factory = compositorFactories.get(factoryName)
    if (factory.isDefined) {
      val namespace = flow.split("/").dropRight(1).mkString("/")
      factory.get.create(definition.getContent)(getResource(namespace))
    } else {
      null
    }
  }

  override def getFlow(flow: String): Flow = flowCache(flow)

  override def getController(flow: String): Option[ExperimentController] = controllerCache.get(flow)

  private def getFlowConf(flow: String): FlowConf = flowConfCache.getOrElse(flow, FlowConf())

  override def onConfUpdate(): Unit = {
    load(reloadFile = false)
  }

  private def load(reloadFile: Boolean, url: String = ""): Unit = synchronized {
    if (reloadFile) {
      loadBuiltin(url)
      loadResource(url)
      loadFlow(url)
    }

    loadConf()
    loadController()
    buildFlow()
  }

  private def loadFlow(url: String): Unit = {
    val (_, files) = fileStore.listFiles(url)

    val flowFiles = files.filter(_.getAbsolutePath.endsWith(".flow"))
    val flows = flowFiles.flatMap { file =>
      try {
        val charStream = CharStreams.fromPath(file.toPath)
        val compiler = new FlowCompiler()
        val flow: FlowDef = compiler.compile(charStream)
        info(("msg", s"load flow def"), ("file", file.getName))
        Some(flow.getName -> flow)
      } catch {
        case e: Throwable =>
          e.printStackTrace()
          error(("msg", "load flow error"), ("path", s"${file.getAbsolutePath}"), ("ex", e.toString))
          None
      }
    }
    val m = flows.toMap
    assert(flows.size == m.size, s"flow store has duplicated flow name: " +
      s"${flows.groupBy(_._1).filter(_._2.size > 1).keys.toSeq.sorted.mkString(",")}")
    flowDefMap = m
  }

  private def buildFlow(): Unit = {
    for ((name, flowDef) <- flowDefMap) {
      try {
        val newFlow = Flow.apply(flowDef, getFlowConf(name))
        flowCache.update(name, newFlow)
        info(("msg", "build flow"), ("name", name))
      } catch {
        case cause: Throwable =>
          error(("msg", "build flow error"), ("name", name), ("ex", cause.getCause), ("detail", cause.toString))
          throw cause
      }
    }

    for (removed <- flowCache.keySet -- flowDefMap.keySet) {
      flowCache.remove(removed)
      info(("msg", "remove flow"), ("name", removed))
    }
  }

  private def loadConf(): Unit = {
    val logMap = configStore.getLogConfList.flatMap ({ conf =>
      val parsed = conf.parseLogConf()
      parsed.assigns.map(assign => (assign.flow -> conf)) ++ Seq(parsed.flow -> conf)
    }).groupBy(_._1).mapValues(_.map(_._2).distinct)

    val subscribeMap= configStore.getSubscriptionConfList.groupBy(_.getString("publisher"))
    val experimentMap = configStore.getExperimentConfList.groupBy(_.getString("flow"))

    val updatedKeys = (logMap.keys ++ subscribeMap.keys ++ experimentMap.keys).toSet
    val oldKeys = flowConfCache.keySet
    for (name <- updatedKeys) {
      flowConfCache.update(name,
        FlowConf(logMap.getOrElse(name, Nil),
          subscribeMap.getOrElse(name, Nil),
          experimentMap.getOrElse(name, Nil)
        )
      )
      info((s"msg", "load flow conf"), ("flow", name))
    }

    for (name <- oldKeys -- updatedKeys) {
      flowConfCache.remove(name)
      info((s"msg", "remove flow conf"), ("flow", name))
    }
  }

  private def loadController(): Unit = {
    val oldKeys = controllerCache.keySet
    configStore.getExperimentConfList.foreach({ conf =>
      val flow = conf.getString("flow")
      val catalog = conf.getString("catalog")
      val enabled = conf.getBoolean("enabled")
      if (enabled) {
        controllerCache.update(flow, controllerFactories(catalog).create(conf)(getBuiltIn))
        info((s"msg", "load controller"), ("flow", flow), ("catalog", catalog))
      } else {
        controllerCache.remove(flow)
        info((s"msg", "turn off controller"), ("flow", flow), ("catalog", catalog))
      }
    })

    val newKeys = controllerCache.keySet
    for (removed <- oldKeys -- newKeys) {
      controllerCache.remove(removed)
      info((s"msg", "remove controller"), ("flow", removed))
    }
  }

  private def loadBuiltin(url: String = ""): Unit = {
    val newValue = if (extBuiltIn.isDefined) {
      new LocalJythonBuiltInV2(fileStore, url).mergeFrom(extBuiltIn.get).mergeFrom(DefaultBuiltIn.defaultBuiltin)
    } else {
      new LocalJythonBuiltInV2(fileStore, url).mergeFrom(DefaultBuiltIn.defaultBuiltin)
    }

    builtIn.set(newValue)
  }

  private def loadResource(url: String): Unit = {
    resourceStore.set(new ResourceStore(fileStore, url))
  }

  override def api: Route = {
    import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
    import akka.http.scaladsl.server.Directives._
    import spray.json._

    concat(
      get {
        path("list") {
          complete(flowCache.keys.toSeq)
        }
      },
      get {
        path("graph") {
          complete(
            new FlowGraphBuilder(flowCache.values.toSeq).build().toJson.parseJson
          )
        }
      },
      get {
        path("controllers") {
          complete(
            controllerFactories.keys.toSeq
          )
        }
      },
      post {
        path("reload") {
          entity(as[String]) { url: String =>
            load(reloadFile = true, url)
            complete(StatusCodes.OK)
          }
        }
      }
    )
  }

}

object GitFlowStore {

  class ResourceStore(fileStore: FileStore, url: String = "") extends Logging {
    var resources: Map[String, Array[Byte]] = Map.empty
    private val (prefix, files) = fileStore.listFiles(url)

    loadResources()

    def loadResources(): Unit = {
      resources = files.filterNot(
        f => f.getAbsolutePath.endsWith(".flow") || f.getAbsolutePath.endsWith(".py")
      ).map { file =>
        info(("msg", "load resource"), ("name", file.getName), ("absolutePath", file.getAbsolutePath))
        file.getAbsolutePath -> FileUtils.readFileToByteArray(file)
      }.toMap
    }

    // absolute path starts with /, relative path is resolved against from the given namespace
    def getResource(namespace: String)(path: String): Array[Byte] = {
      var realPath = ""
      try {
         realPath = if (path.startsWith("/")) {
          Paths.get(prefix, path).toRealPath().toString
        } else {
          Paths.get(prefix, namespace).resolve(path).toRealPath().toString
        }
        resources(realPath)
      } catch {
        case e: Throwable =>
          error(("msg", "get resource error"), ("namespace", namespace), ("path", path), ("realpath", realPath))
          throw e
      }
    }
  }

  def newBuilder(): FlowStoreBuilder = {
    new FlowStoreBuilder()
  }

  class FlowStoreBuilder {
    private val cf: mutable.Map[String, CompositorFactory] = mutable.Map[String, CompositorFactory]()
    private var bi: Option[BuiltIn] = None
    private var cs: ConfigStore = _
    private var ecf: mutable.Map[String, ControllerFactory] = mutable.Map[String, ControllerFactory]()
    private var fs: FileStore = _

    def build(): FlowStore = {
      new GitFlowStore(cs, fs, cf.toMap, ecf.toMap, bi)
    }

    def withBuiltIn(builtIn: BuiltIn): this.type = {
      bi = Some(builtIn)
      this
    }

    def withExperimentControllerFactory(factory: ControllerFactory): this.type = {
      ecf += (factory.name -> factory)
      this
    }

    def withConfigStore(configStore: ConfigStore): this.type = {
      cs = configStore
      this
    }

    def withCompositorFactory(name: String, factory: CompositorFactory): this.type = {
      cf.put(name, factory)
      this
    }

    def withFileStore(fileStore: FileStore): this.type = {
      fs = fileStore
      this
    }

  }
}
