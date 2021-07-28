package com.didichuxing.horoscope.service.storage

import java.util.concurrent.atomic.AtomicReference

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import com.didichuxing.horoscope.core.FlowConf
import com.didichuxing.horoscope.core.FlowDslMessage.{CompositorDef, FlowDef}
import com.didichuxing.horoscope.core._
import com.didichuxing.horoscope.dsl.FlowCompiler
import com.didichuxing.horoscope.runtime.experiment.{ControllerFactory, ExperimentController}
import com.didichuxing.horoscope.runtime.expression.{BuiltIn, DefaultBuiltIn, Expression, LocalJythonBuiltInV2}
import com.didichuxing.horoscope.util.FlowConfParser._
import com.didichuxing.horoscope.util.{FlowGraph, Logging, Utils}
import org.antlr.v4.runtime.CharStreams
import com.typesafe.config.{Config, ConfigValueFactory}
import spray.json.DefaultJsonProtocol.{StringJsonFormat, seqFormat}

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class GitFlowStore(
  configStore: ConfigStore,
  fileStore: FileStore,
  compositorFactories: Map[String, CompositorFactory],
  controllerFactories: Map[String, ControllerFactory],
  extBuiltIn: Option[BuiltIn] = None
) extends FlowStore with ConfigChangeListener with ConfigChecker with Logging {
  import GitFlowStore._
  import ZookeeperConfigStore._

  // flowName -> FlowDef
  private var flowDefMap: Map[String, FlowDef] = Map.empty
  // flowName -> FlowConf
  private var flowConfCache: Map[String, FlowConf] = Map.empty
  // flowName -> Flow
  private var flowCache: Map[String, Flow] = Map.empty
  // flowName -> Controller
  private var controllerCache: Map[String, Seq[ExperimentController]] = Map.empty
  private val builtIn: AtomicReference[BuiltIn] = new AtomicReference[BuiltIn]()
  private val resourceStore: AtomicReference[ResourceStore] = new AtomicReference[ResourceStore]()
  private var flowGraph = FlowGraph.newBuilder(flowCache.values.toSeq).build()

  configStore.registerListener(this)
  configStore.registerChecker(this)
  load(reloadFile = true)

  override implicit def getBuiltIn: BuiltIn = builtIn.get()

  def getResource(namespace: String)(path: String): Array[Byte] = resourceStore.get().get(namespace)(path)

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

  override def getController(flow: String): Seq[ExperimentController] = controllerCache.getOrElse(flow, Nil)

  private def getFlowConf(flow: String): FlowConf = flowConfCache.getOrElse(flow, FlowConf())

  override def getFlowGraph: FlowGraph = flowGraph

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
    val builder = Map.newBuilder[String, Flow]
    for ((name, flowDef) <- flowDefMap) {
      try {
        val newFlow = Flow.apply(flowDef, getFlowConf(name))
        builder += (name -> newFlow)
        info(("msg", "build flow"), ("name", name))
      } catch {
        case cause: Throwable =>
          error(("msg", "build flow error"), ("name", name), ("ex", cause.getCause), ("detail", cause.toString))
          throw cause
      }
    }

    flowCache = builder.result
    flowGraph = FlowGraph.newBuilder(flowCache.values.toSeq).build()
  }

  private def loadConf(): Unit = {
    var builder = Map.newBuilder[String, FlowConf]
    val logConf = configStore.getConfList(LOG_TYPE)
    val subscribeMap= configStore.getConfList(SUBSCRIPTION_TYPE).groupBy(_.getString("publisher"))
    val experimentMap = configStore.getConfList(EXPERIMENT_TYPE).groupBy(_.getString("flow"))
    val callbackMap = configStore.getConfList(CALLBACK_TYPE).groupBy(_.getString("flow.register"))

    for (name <- flowDefMap.keys) {
      builder += (
        name -> FlowConf(logConf,
          subscribeMap.getOrElse(name, Nil),
          experimentMap.getOrElse(name, Nil),
          callbackMap.getOrElse(name, Nil)
        ))
      info((s"msg", "load flow conf"), ("flow", name))
    }

    flowConfCache = builder.result()
  }

  private def loadController(): Unit = {
    val result = mutable.Map[String, List[ExperimentController]]()
    configStore.getConfList(EXPERIMENT_TYPE).foreach({ conf =>
      val flow = conf.getString("flow")
      val catalog = conf.getString("catalog")
      val enabled = conf.getBoolean("enabled")
      if (enabled) {
        val controller = controllerFactories(catalog).create(conf)(getBuiltIn)
        result += (flow -> (controller :: result.getOrElse(flow, Nil)))
        info((s"msg", "load controller"), ("flow", flow), ("catalog", catalog))
      }
    })
    controllerCache = result.toMap
  }

  private def loadBuiltin(url: String = ""): Unit = {
    val newValue = if (extBuiltIn.isDefined) {
      new LocalJythonBuiltInV2(fileStore, url)
        .mergeFrom(extBuiltIn.get)
        .mergeFrom(DefaultBuiltIn.defaultBuiltin)
    } else {
      new LocalJythonBuiltInV2(fileStore, url)
        .mergeFrom(DefaultBuiltIn.defaultBuiltin)
    }

    builtIn.set(newValue)
  }

  private def loadResource(url: String): Unit = {
    resourceStore.set(new ResourceStore(fileStore, url))
  }

  private def checkExpression(expressions: Map[String, Seq[String]]): Boolean = {
    for ((flowName, exprs) <- expressions) {
      val flow = getFlow(flowName)
      val flowVariables = flow.variables.keySet ++ flow.arguments.keySet.map("@" + _) ++ "@event_id"
      val refs = exprs.flatMap(Expression(_).references.map(_.name))
      val inValid = refs.find(!flowVariables.contains(_))
      assert(inValid.isEmpty, s"variable ${inValid.get} does not exist in flow ${flow.name}")
    }
    true
  }

  override def check(name: String, confType: String, conf: Config): Boolean = {
    checkConfName(name, conf)
    confType match {
      case LOG_TYPE =>
        // check flow tags
        checkLogConf(conf)
        checkExpression(conf.parseLogConf().expressions)
      case SUBSCRIPTION_TYPE =>
        checkExpression(conf.parseSubscribe().expressions)
      case CALLBACK_TYPE =>
        checkExpression(conf.parseCallbackConf().expressions)
      case EXPERIMENT_TYPE =>
        // check experiment traffic and priority
        checkExperimentConf(conf, configStore.getConfList(confType))
        checkExpression(conf.parseABTestConf().expressions)
    }
  }

  override def api: Route = {
    import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
    import akka.http.scaladsl.server.Directives._
    import spray.json._

    concat(
      get {
        path("list") {
          complete(flowCache.keys.toSeq.sorted)
        }
      },
      get {
        path("graph") {
          complete(
            getFlowGraph.toJson.parseJson
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
            Utils.run {
              load(reloadFile = true, url)
              complete(StatusCodes.OK)
            }
          }
        }
      }
    )
  }



}

object GitFlowStore {

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

  // distribute traffic in order of priority
  // The smaller the priority number, the higher the priority.
  def distributeTraffic(configs: Seq[Config]): Seq[Config] = {
    val sorted = configs.view.filter(_.getBoolean("enabled")).sortBy(_.getInt("priority"))
    val newConfigs = new ListBuffer[Config]()
    sorted.foldLeft(0)((sum, config) => {
      val original = config
      val bucket = original.getIntList("traffic").asScala.map(_.intValue())
      val newBucket = bucket.map(_ + sum)
      val result = sum + bucket.last -  bucket.head
      val newConfig = original.withValue("traffic", ConfigValueFactory.fromIterable(newBucket))
      newConfigs.add(newConfig)
      result
    })

    newConfigs
  }
}
