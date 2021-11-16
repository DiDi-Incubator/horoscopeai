package com.didichuxing.horoscope.runtime

import java.nio.file.Paths
import java.util.concurrent.Callable

import akka.actor.ActorSystem
import com.didichuxing.horoscope.core.FlowConf
import com.didichuxing.horoscope.core.FlowDslMessage.CompositorDef
import com.didichuxing.horoscope.core.FlowRuntimeMessage._
import com.didichuxing.horoscope.core.{Compositor, Flow}
import com.didichuxing.horoscope.dsl.FlowCompiler
import com.didichuxing.horoscope.runtime.Implicits.builtin
import com.didichuxing.horoscope.runtime.experiment.{ABTestController, ExperimentController}
import com.didichuxing.horoscope.runtime.expression.{BuiltIn, DefaultBuiltIn, Expression}
import com.didichuxing.horoscope.util.{FileUtils, FlowGraph, Utils}
import com.google.common.io.Resources
import com.typesafe.config.{Config, ConfigFactory}
import org.antlr.v4.runtime.CharStreams

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Future

// scalastyle:off
class ChoreographySuite extends ExecutorSuiteHelper {

  class MockEnv extends Environment {
    private val compositors: mutable.Map[(String, String), Compositor] = mutable.Map()
    private val contexts: mutable.Map[String, Map[String, TraceVariable]] = mutable.Map()
    private val flowConfigs = loadConf()
    private val controllers: Map[String, Seq[ExperimentController]] = loadFlowControllers()
    private val flows: Map[String, Flow] = loadFlows()

    private def getFlowConf(flow: String): FlowConf = {
      val subscribeConf = flowConfigs._2.filter(_.getString("publisher") == flow)
      val exprConf = flowConfigs._3.filter(_.getString("flow") == flow)
      val topicConf = flowConfigs._4
      val callbackConf = flowConfigs._5.filter(_.getString("flow.register") == flow)

      FlowConf(topicConf, subscribeConf, exprConf, callbackConf)
    }

    override def getBuiltIn(): BuiltIn = DefaultBuiltIn.defaultBuiltin

    private def loadConf(): (Seq[Config], Seq[Config], Seq[Config], Seq[Config], Seq[Config]) = {
      val logConf = ConfigFactory.load("flow-conf/log.conf").getConfigList("logs").asScala
      val experimentConf = ConfigFactory.load("flow-conf/experiment.conf")
        .getConfigList("experiments").asScala
      val subscribeConf = ConfigFactory.load("flow-conf/subscribe.conf")
        .getConfigList("subscriptions")

      val topicConf = ConfigFactory.load("flow-conf/topic.conf").getConfigList("topics")
      val callbackConf = ConfigFactory.load("flow-conf/callback.conf").getConfigList("callbacks")
      (logConf, subscribeConf, experimentConf, topicConf, callbackConf)
    }

    private def loadFlowControllers(): Map[String, Seq[ExperimentController]] = {
      val result = flowConfigs._3.map({ conf =>
        val flow = conf.getString("flow")
        val catalog = conf.getString("catalog")
        val controller = catalog match {
          case "ab_test" => new ABTestController(conf)(builtin)
          case _ => null
        }
        flow -> controller
      }).groupBy(_._1).mapValues(r => r.map(_._2))

      result
    }

    def loadFlows(): Map[String, Flow] = {
      val uri = Resources.getResource("infoflow/v2/").toURI
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

    override def getController(flow: String): Seq[ExperimentController] = controllers.getOrElse(flow, Nil)

    override def getFlowByName(name: String): Flow = flows(name)

    override def getFlowGraph(): FlowGraph = FlowGraph.newBuilder(flows.values.toSeq).build()

    def getTraceContext(traceId: String, keys: Array[String]): Future[Map[String, TraceVariableOrBuilder]] = {
      Future.successful(contexts.getOrElse(traceId, Map.empty))
    }
  }

  var env: MockEnv = _

  before {
    env = new MockEnv()
    executor = new FlowExecutorImpl(ConfigFactory.parseString(
      """
        |horoscope.flow-executor.topic-log.enabled = true
        |""".stripMargin
    ), ActorSystem("flow-test"), env)

    executor.start()
  }

  after {
    executor.stop()
    env = null
    executor = null
  }

  def run(event: FlowEvent)(f: FlowInstance => Unit): Unit = {
    whenReady(executor.execute(event)) { instance =>
      f(instance)
    }
  }

  def getLocalTrace(): Map[String, TraceVariable] = {
    val traceContext = executor.contextCache.get("a", new Callable[Map[String, TraceVariable]] {
      def call(): Map[String, TraceVariable] = Map.empty
    })
    traceContext
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

  /**
   * /v2/topic/log --(include)-> /v2/topic/log-include
   *        |
   *        |
   *         --(schedule)--> /v2/topic/log-schedule --(include)-> /v2/topic/recursion <-----
   *                                                     \                           \       \
   *                                               /v2/topic/hello                    (include)
   */
  test("flow topic") {
    // test forward context
    // env.updateContext("a", "1", "$delta", Value(0))
    val event = newEvent("1", "a")("v2/topic/log", "input" -> Value(-1))
    var schedule: FlowEvent = null

    run(event) { instance =>
      schedule = instance.getSchedule(0)
      // topic1 in forward state
      instance("""schedule[0].forward.length()""").as[Int] should be > 0
      instance("""schedule[0].forward[1].topic_name """).as[String] shouldBe "topic1"
      // instance("""schedule[0].forward.variable.v_input""").as[Int] shouldBe -1
      instance("""schedule[0].forward[1].tag.length()""").as[Int] should be > 0

      // topic2 in backward state
      instance(""" backward[0].topic_name """).as[String] shouldBe "topic2"
      instance("""backward[0].dependency_flow  """).as[Seq[String]] should contain theSameElementsAs(
        Seq("/v2/topic/hello", "/v2/topic/recursion", "/v2/topic/base"))
      instance("""schedule[0].trigger[0].topic_name """).as[String] shouldBe "topic2"
      instance("""backward[0].tag.length()""").as[Int] should be > 0

    }

    run(schedule) { instance =>
      // topic1 is finished
      instance("""topic[?(_.topic_name == "topic1")][-][0].field.length() """).as[Int] shouldBe 8
      instance("""topic[?(_.topic_name == "topic1")][-][0].field.c_include """).as[Seq[String]] should contain theSameElementsAs
        Seq("flag")

      // topic2 is finished
      instance("""topic[?(_.topic_name == "topic2")][-][0].field.length() """).as[Int] shouldBe 10
      instance("""topic[?(_.topic_name == "topic2")][-][0].field.c_include """).as[Seq[String]] should contain theSameElementsAs
        Seq("flag")
      instance("""topic[?(_.topic_name == "topic2")][-][0].field.t_base """).as[Boolean] shouldBe false

      // 后向flow记录最早一次执行
      instance("""topic[?(_.topic_name == "topic2")][-][0].field.c_recursion """).as[Seq[String]] shouldBe Seq("continue")

      // 中心flow多次执行都记录
      instance("""topic[?(_.topic_name == "topic4")].length()""").as[Int] should be > 0

      // 前向flow记录最后一次执行
      instance("""topic[?(_.topic_name == "topic5")][-].length()""").as[Int] shouldBe 1
      instance("""topic[?(_.topic_name == "topic5")][-][0].field.c_recursion """).as[Seq[String]] shouldBe Seq("finish")

      // topic6, 测试多个forwardContext
      instance("""topic[?(_.topic_name == "topic6")][-].length()""").as[Int] shouldBe 1
      instance("""topic[?(_.topic_name == "topic6")][-][0].field.t_log """).as[Boolean] shouldBe true

      // delete backward context
      val logIds = instance("""topic.log_id """).as[Seq[String]].map("&" + _)
      instance("""delete.length() """).as[Int] should be > 0
      instance("""delete """).as[Seq[String]] should contain theSameElementsAs(logIds)
      getLocalTrace().keys.toSeq should contain theSameElementsAs(Seq("$"))
    }
  }

  /**
   *               -- (schedule) --> /v2/callback/schedule
   *              /
   * /v2/callback/register --(callback)--> /v2/callback/callback
   *              \
   *               --(timeout)--> /v2/callback/timeout
   *
   *
   * /v2/callback/source --(include)-> /v2/callback/callback --(include)--> /v2/callback/hello <----
   *                                                                                         \      \
   *                                                                                         (include)
   */

  test("callback in time") {
    val token = "12345"
    val register = newEvent("1", "a")("v2/callback/register", ("token", Value(token)))
    val callback = newEvent("1", "a")("v2/callback/source", ("token", Value(token)))
    var timeout: FlowEvent = null
    var schedule: FlowEvent = null
    run(register) { instance =>
      // token context for callback
      instance.getToken(0).getArgumentMap.asScala.mapValues(Value(_)) shouldBe
        Map("link" -> Value(1), "importance" -> Value(1))
      // generate timeout event with token
      timeout = instance.getSchedule(1)
      timeout.getFlowName shouldBe "/v2/callback/timeout"
      timeout.getToken shouldBe "#" + token
      schedule = instance.getSchedule(0)
      getLocalTrace().keySet should contain("#" + token)
    }

    // 测试在callback 或者 callback timeout之间有其他schedule事件执行的情况
    run(schedule) {instance =>
      instance("""topic""").as[Value] shouldBe NULL
    }

    // callback flow executes with context args
    run(callback) { instance =>
      val callbackFlowArgs = instance.getProcedure(1).getArgumentMap.asScala
      Value(callbackFlowArgs("#")).as[ValueDict].toMap shouldBe
        Map("link" -> Value(1), "importance" -> Value(1))
      getLocalTrace().keySet should not contain ("#" + token)

      // after callback, topic3 is complete
      instance("""topic[0].topic_name """).as[String] shouldBe "topic3"
      instance("""topic[0].field.length() """).as[Int] shouldBe 9
      instance("""topic[0].field.t_callback """).as[Boolean] shouldBe true
      // instance("""topic[0].field.t_timeout """).as[Boolean] shouldBe false
    }

    // timeout flow got fault
    run(timeout) { instance =>
      instance.getProcedure(0).getFaultCount shouldBe 1
    }
  }

  test("callback out of time") {
    val token = "12345"
    val register = newEvent("1", "a")("v2/callback/register", ("token", Value(token)))
    val callback = newEvent("1", "a")("v2/callback/source", ("token", Value(token)))
    var timeout: FlowEvent = null
    var schedule: FlowEvent = null
    run(register) { instance =>
      timeout = instance.getSchedule(1)
      schedule = instance.getSchedule(0)
    }

    // 测试在callback 或者 callback timeout之间有其他schedule事件执行的情况
     run(schedule) { instance =>
      instance("""topic""").as[Value] shouldBe NULL
     }

    // timeout flow executed with context args
    run(timeout) { instance =>
      val callbackFlowArgs = instance.getProcedure(0).getArgumentMap.asScala
      Value(callbackFlowArgs("#")).as[ValueDict].toMap shouldBe
        Map("link" -> Value(1), "importance" -> Value(1))
      getLocalTrace().keySet should not contain ("#" +  token)

      // topic3 is finished
      instance("""topic[0].topic_name """).as[String] shouldBe "topic3"
      instance("""topic[0].field.length() """).as[Int] shouldBe 6
      instance("""topic[0].field.t_callback """).as[Boolean] shouldBe false
      // instance("""topic[0].field.t_timeout """).as[Boolean] shouldBe true
    }

    // callback flow got fault
    run(callback) { instance =>
      instance.getProcedure(0).getFaultCount should be > 0
    }
  }

  test("multi callback event") {
    val start = newEvent("1", "a")("v2/callback/start")
    var callback = newEvent("1", "a")("v2/callback/source", ("token", Value("model_a")))
    var callback2 = newEvent("1", "a")("v2/callback/source", ("token", Value("model_b")))
    var timeout: FlowEvent = null
    var timeout2: FlowEvent = null

    run(start) { instance =>
      timeout = instance.getScheduleList.filter(a =>
        a.getFlowName == "/v2/callback/timeout" && a.getToken == "#model_a").head
      timeout2 = instance.getScheduleList.filter(a =>
        a.getFlowName == "/v2/callback/timeout" && a.getToken == "#model_b").head
      instance.getScheduleCount shouldBe 4
      instance.getBackwardCount shouldBe 2
      instance.getTokenCount shouldBe 2
    }

    run(timeout) { instance =>
      instance.getTopicCount shouldBe 1
      instance.getTopicList.head.getScopeList should contain theSameElementsAs Seq("main", "model_a", "register_a")
    }

    run(callback) { instance =>
        instance.getTopicCount shouldBe 0
    }

    run(callback2) { instance =>
      instance.getTopicCount shouldBe 1
      instance.getTopicList.head.getScopeList should contain theSameElementsAs Seq("main", "model_b", "register_b")
    }

    run(timeout2) { instance =>
      instance.getTopicCount shouldBe 0
    }

  }

  /**
   * /v2/experiment/entry --(include)-> /v2/experiment/hello
   *      \
   *       --(schedule)--> /v2/experiment/hello
   *
   * /v2/experiment/entry-expt --(include)--> /v2/experiment/hello
   *       \
   *        --(schedule)--> /v2/experiment/hello
   */
  test("experiment") {
    val eventId = "1"
    // bucket 26
    val event = newEvent("1", "a")("v2/experiment/entry", ("input" -> Value(0)))
    var scheduleEvent: FlowEvent = null
    run(event) { instance =>
      // test experiment traffic and condition
      instance("""procedure[0].experiment.name """).as[String] shouldBe "test-experiment-2"
      instance("""procedure[0].flow_name""").as[String] shouldBe "/v2/experiment/entry-expt"
      instance("""procedure[0].argument.tag """).as[String] shouldBe "treatment"
      instance("""procedure[0].experiment.group """).as[String] shouldBe "treatment"

      // include flow inherit experiment plan from parent procedure
      instance("""procedure[1].flow_name""").as[String] shouldBe "/v2/experiment/hello-expt"
      instance("""procedure[1].experiment.group""").as[String] shouldBe "treatment"

      // schedule event with experiment plan and dependency
      instance("""schedule[0].flow_name """).as[String] shouldBe "/v2/experiment/hello"
      instance("""schedule[0].experiment.dependency.result """).as[String] shouldBe "this is treatment"
      instance("""schedule[0].experiment.plan.length() """).as[Int] shouldBe 2

      scheduleEvent = instance.getSchedule(0)
    }

    // run schedule event with inherited experiment plan
    run(scheduleEvent) { instance =>
      instance("""procedure[0].flow_name""").as[String] shouldBe "/v2/experiment/hello-expt"
      instance("""procedure[0].experiment.name """).as[String] shouldBe "test-experiment-2"
      instance("""procedure[0].argument.result """).as[String] shouldBe "this is treatment!"
    }
  }

  test("flow graph") {
    val flowGraph = FlowGraph.newBuilder(flows = new MockEnv().loadFlows().values.toSeq).build()
    val json = flowGraph.toJson

    // flow graph contains edge from register flow to callback flow
    val path1 = flowGraph.getPath("/v2/callback/register", "/v2/callback/callback")
    path1 should contain theSameElementsAs Seq("/v2/callback/register", "/v2/callback/callback")

    // flow graph does not contain edge from register flow to timeout flow
    val path2 = flowGraph.getPath("/v2/callback/register", "/v2/callback/timeout")
    path2 should contain noElementsOf Seq("/v2/callback/register", "/v2/callback/timeout")

    val path3 = flowGraph.getPath("/v2/topic/log", "/v2/topic/hello")
    path3 should contain theSameElementsAs
      Seq("/v2/topic/log", "/v2/topic/log-schedule", "/v2/topic/recursion", "/v2/topic/hello")
  }
}
