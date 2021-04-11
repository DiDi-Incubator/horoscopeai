package com.didichuxing.horoscope.runtime

import java.nio.file.Paths

import akka.actor.ActorSystem
import com.didichuxing.horoscope.core.Flow.FlowConf
import com.didichuxing.horoscope.core.FlowDslMessage.CompositorDef
import com.didichuxing.horoscope.core.{Compositor, Flow}
import com.didichuxing.horoscope.core.FlowRuntimeMessage._
import com.didichuxing.horoscope.dsl.FlowCompiler
import com.didichuxing.horoscope.runtime.Implicits.builtin
import com.didichuxing.horoscope.runtime.experiment.{ABTestController, ExperimentController}
import com.didichuxing.horoscope.util.{FileUtils, FlowGraphBuilder}
import com.google.common.io.Resources
import com.typesafe.config.{Config, ConfigFactory}
import org.antlr.v4.runtime.CharStreams
import com.didichuxing.horoscope.util.FlowConfParser._

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.Future

// scalastyle:off
class ChoreographySuite extends ExecutorSuiteHelper {

  class MockEnv extends Environment {
    private val compositors: mutable.Map[(String, String), Compositor] = mutable.Map()
    private val contexts: mutable.Map[String, Map[String, TraceVariable]] = mutable.Map()
    private val flowConfigs = loadConf()
    private val controllers: Map[String, ExperimentController] = loadFlowControllers()
    private val flows: Map[String, Flow] = loadFlows()

    private def getFlowConf(flow: String): FlowConf = {
      val flowLogConf = flowConfigs._1.filter({ conf =>
        val parsed = conf.parseLogConf()
        parsed.flow == flow || parsed.assigns.toSet.exists(_.flow == flow) || parsed.choices.contains(flow)
      })

      val subscribeConf = flowConfigs._2.filter(_.getString("publisher") == flow)
      val exprConf = flowConfigs._3.filter(_.getString("flow") == flow)

      FlowConf(flowLogConf, subscribeConf, exprConf)
    }

    private def loadConf(): (Seq[Config], Seq[Config], Seq[Config]) = {
      val logConf = ConfigFactory.load("flow-conf/log.conf").getConfigList("logs").asScala
      val experimentConf = ConfigFactory.load("flow-conf/experiment.conf")
        .getConfigList("experiments").asScala
      val subscribeConf = ConfigFactory.load("flow-conf/subscribe.conf")
        .getConfigList("subscriptions")

      (logConf, subscribeConf, experimentConf)
    }

    private def loadFlowControllers(): Map[String, ExperimentController] = {
      val result = flowConfigs._3.map({ conf =>
        val flow = conf.getString("flow")
        val catalog = conf.getString("catalog")
        val controller = catalog match {
          case "ab_test" => new ABTestController(conf)(builtin)
          case _ => null
        }
        flow -> controller
      }).toMap

      result
    }

    def loadFlows(): Map[String, Flow] = {
      val uri = Resources.getResource("infoflow").toURI
      val basePath = Paths.get(uri).normalize()
      val flowFiles = FileUtils.walkFileTree(basePath, Set(".flow"))

      val result = flowFiles.map { f =>
        val code = CharStreams.fromPath(f.toPath)
        val compiler = new FlowCompiler()
        val flowDef = compiler.compile(code)
        val flowName = flowDef.getName
        val flowConf = getFlowConf(flowName)
        val flow = Flow(flowDef, flowConf)
        flowName -> flow
      }.toMap

      result
    }

    def updateContext(traceId: String, eventId: String, name: String, value: Value): Unit = {
      val traceVariable = TraceVariable.newBuilder()
        .setValue(value.as[FlowValue])
        .setReference(
          ValueReference.newBuilder().setEventId(eventId).setName(name)
        )
        .build()

      val context = contexts.getOrElse(traceId, Map.empty)
      contexts.update(traceId, context.updated(name, traceVariable))
    }

    def compositor(factory: String, code: String): Compositor = {
      compositors.getOrElseUpdate((factory, code), mock[Compositor])
    }

    implicit def buildCompositor: (String, CompositorDef) => Compositor = { (flow, definition) =>
      compositor(definition.getFactory, definition.getContent)
    }

    override def getController(flow: String): Option[ExperimentController] = controllers.get(flow)

    override def getFlowByName(name: String): Flow = {
      flows(name)
    }

    def getTraceContext(traceId: String, keys: Array[String]): Future[Map[String, TraceVariableOrBuilder]] = {
      Future.successful(contexts.getOrElse(traceId, Map.empty))
    }
  }

  var env: MockEnv = _

  before {
    env = new MockEnv()
    executor = new FlowExecutorImpl(ConfigFactory.parseString(
      """
        |horoscope.flow-executor.log.redirected = true
        |""".stripMargin
    ), ActorSystem("flow-test"), env)

    executor.start()
  }

  after {
    executor.stop()
    env = null
    executor = null
  }

  test("subscribe") {
    def run(level: Int, eventId: String)(f: FlowInstance => Unit): Unit = {
      val event = newEvent(eventId, "a")("v2/publish", "level" -> Value(level))
      whenReady(executor.execute(event)) { instance => f(instance) }
    }

    // bucket: 26, traffic condition is satisfied
    run(level = 1, eventId = "1") { instance =>
      instance.getProcedureList.asScala.length shouldBe 1
    }

    // bucket: 87, traffic condition is not satisfied
    run(level = 3, eventId = "3") { instance =>
      instance.getProcedureList.asScala.length shouldBe 1
    }

    // bucket: 26, traffic condition is satisfied
    run(level = 3, eventId = "1") { instance =>
      instance.getProcedureList.asScala.length shouldBe 2
      instance("""procedure[1].flow_name""").as[String] shouldBe "/v2/subscribe"
      instance("""procedure[1].argument.input""").as[Double] shouldBe 75.0 +- 0.01
      instance("""procedure[1].argument.output""").as[String] shouldBe "a"
    }
  }

  test("flow experiment") {
    def run(level: Int, eventId: String, flow: String)(f: FlowInstance => Unit): Unit = {
      val event = newEvent(eventId, "a")(flow, "level" -> Value(level), "tag" -> Value("unknown"))
      whenReady(executor.execute(event)) { instance => f(instance) }
    }

    // experiment condition is not satisfied, bucket=26, fallback to default
    run(level = 1, eventId = "1", flow = "v2/base") { instance =>
      instance("""procedure[0].flow_name""").as[String] shouldBe "/v2/base"
      instance("""procedure[0].experiment.name""").as[String] shouldBe "base-experiment"
      instance("""procedure[0].experiment.group""").as[String] shouldBe "default_group"
      instance("""procedure[0].argument.tag""").as[String] shouldBe "unknown"
    }

    // experiment condition is satisfied, bucket=26, choose control group
    run(level = 3, eventId = "1", flow = "v2/base") { instance =>
      instance("""procedure[0].flow_name""").as[String] shouldBe "/v2/base"
      instance("""procedure[0].experiment.name""").as[String] shouldBe "base-experiment"
      instance("""procedure[0].experiment.group""").as[String] shouldBe "control-group"
      instance("""procedure[0].argument.tag""").as[String] shouldBe "control"
    }

    // experiment condition is satisfied, bucket=87, choose treatment group
    run(level = 3, eventId = "3", flow = "v2/base") { instance =>
      instance("""procedure[0].experiment.name""").as[String] shouldBe "base-experiment"
      instance("""procedure[0].experiment.group""").as[String] shouldBe "treatment-group"
      instance("""procedure[0].argument.tag""").as[String] shouldBe "treatment"
    }
  }

  test("include experiment") {
    def run(level: Int, eventId: String, flow: String = "v2/entry")(f: FlowInstance => Unit): Unit = {
      val event = newEvent(eventId, "a")(flow, "level" -> Value(level))
      whenReady(executor.execute(event)) { instance => f(instance) }
    }

    // experiment condition is not satisfied, fallback to default
    run(level = 1, eventId = "1") { instance =>
      instance("""procedure[1].flow_name""").as[String] shouldBe "/v2/base"
      instance("""procedure[1].experiment.name""").as[String] shouldBe "base-experiment"
      instance("""procedure[1].experiment.group""").as[String] shouldBe "default_group"
    }

    // experiment condition is satisfied, choose treatment group
    run(level = 3, eventId = "3") { instance =>
      instance("""procedure[1].experiment.name""").as[String] shouldBe "base-experiment"
      instance("""procedure[1].experiment.group""").as[String] shouldBe "treatment-group"
      instance("""procedure[1].argument.tag""").as[String] shouldBe "treatment"
    }
  }

  test("schedule experiment") {
    def run(level: Int, eventId: String, flow: String)(f: FlowInstance => Unit): Unit = {
      val event = newEvent(eventId, "a")(flow, "level" -> Value(level))
      whenReady(executor.execute(event)) { instance => f(instance) }
    }

    // experiment condition is satisfied, choose treatment group
    run(level = 3, eventId = "1", flow = "v2/entry") { instance =>
      instance.getSchedule(0).getParent.getExperimentMap.contains("@level") shouldBe true
    }
  }

  test("log") {
    env.updateContext("a", "1", "$delta", Value(0))
    val event = newEvent("1", "a")("v2/log", "input" -> Value(-1))
    whenReady(executor.execute(event)) { instance =>
      instance("""procedure[1].flow_name""").as[String] shouldBe "/v2/log-include"
      instance("""procedure[1].context_choice""").as[Seq[String]] shouldBe (Seq("/v2/log:negative"))
      instance("""procedure[1].assign.input""").as[Int] shouldBe -1
      instance("""procedure[1].assign.delta """).as[Int] shouldBe 1
      instance("""procedure[1].assign.output """).as[String] shouldBe "negative"

      instance("""procedure[0].flow_name """).as[String] shouldBe "/v2/log"
      instance("""schedule[0].parent.variable[?(_.reference.name == "output")][-][0].value""").as[String] shouldBe "negative"
      instance("""schedule[0].parent.choice""").as[Map[String, Map[String, Seq[String]]]] shouldBe
        Map("/v2/log" -> Map("choice" -> Seq("negative")))

    }
  }

  test("flow graph") {
    new FlowGraphBuilder(flows = new MockEnv().loadFlows().values.toSeq).build().toJson
  }
}
