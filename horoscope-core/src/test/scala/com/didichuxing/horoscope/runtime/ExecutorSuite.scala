/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.runtime

import java.nio.charset.Charset
import java.time.LocalDateTime
import java.util.Base64

import akka.actor.ActorSystem
import com.didichuxing.horoscope.core.FlowDslMessage.CompositorDef
import com.didichuxing.horoscope.core.FlowRuntimeMessage._
import com.didichuxing.horoscope.core.{Compositor, Flow}
import com.didichuxing.horoscope.dsl.FlowCompiler
import com.didichuxing.horoscope.runtime.convert.ValueTypeAdapter
import com.didichuxing.horoscope.util.Logging
import com.google.common.io.Resources
import com.google.gson.{Gson, GsonBuilder}
import com.typesafe.config.ConfigFactory
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.language.{implicitConversions, postfixOps}
import scala.util.Try

class ExecutorSuite extends FunSuite
  with Matchers with ScalaFutures
  with MockFactory with BeforeAndAfter
  with Logging {
  import com.didichuxing.horoscope.runtime.Implicits.{builtin, asDocument}
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.collection.JavaConversions._

  implicit val gson: Gson = new GsonBuilder()
    .registerTypeHierarchyAdapter(classOf[Value], new ValueTypeAdapter)
    .setPrettyPrinting()
    .serializeNulls()
    .create()

  implicit val defaultPatience: PatienceConfig =
    PatienceConfig(timeout =  Span(10000, Seconds), interval = Span(5, Millis))

  class MockEnv extends Environment {
    private val flows: mutable.Map[String, Flow] = mutable.Map()
    private val compositors: mutable.Map[(String, String), Compositor] = mutable.Map()
    private val contexts: mutable.Map[String, Map[String, TraceVariable]] = mutable.Map()

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

    implicit def buildCompositor: CompositorDef => Compositor = { definition =>
      compositor(definition.getFactory, definition.getContent)
    }

    override def getFlowByName(name: String): Option[Flow] = {
      {
        flows.get(name)
      } orElse {
        val flow = Try {
          val code = Resources.toString(
            Resources.getResource(s"infoflow$name.flow"), Charset.defaultCharset()
          )
          val flowDef = FlowCompiler.compile(code)
          Flow(flowDef)
        }
        flow.foreach(flows += name -> _)
        Some(flow.get)
      }
    }

    def getTraceContext(traceId: String, keys: Array[String]): Future[Map[String, TraceVariableOrBuilder]] = {
      Future.successful(contexts.getOrElse(traceId, Map.empty))
    }
  }

  val emptyDict: ValueDict = Value(Map.empty[String, Value])

  var env: MockEnv = _

  var executor: FlowExecutorImpl = _

  def newEvent(eventId: String, traceId: String)(flow: String, args: (String, Value)*): FlowEvent = {
    val builder = FlowEvent.newBuilder()
    builder.setEventId(eventId)
    builder.setTraceId(traceId)
    builder.setFlowName("/" + flow)

    for ((key, value) <- args) {
      builder.putArgument("@" + key, TraceVariable.newBuilder().setValue(value.as[FlowValue]).build())
    }

    builder.build()
  }

  implicit class FlowInstanceHelper(instance: FlowInstance) {
    lazy val assigns: Map[String, FlowInstance.Assign] = {
      val keys = instance.getAssignList.map(_.getName)
      assert(keys.distinct.length == keys.length, instance.toString)
      instance.getAssignList.map(a => (a.getName, a)).toMap
    }

    lazy val chooses: Map[String, FlowInstance.Choose] = {
      val keys = instance.getChooseList.map(_.getChoice)
      assert(keys.distinct.length == keys.length)
      instance.getChooseList.map(c => (c.getChoice, c)).toMap
    }

    def apply(code: String): Value = {
      Value(instance).asInstanceOf[ValueDict].eval(code)
    }

    def toJson: String = Value(instance).toJson
  }

  implicit class AssignHelper(assign: FlowInstance.Assign) {
    def value: Value = Value(assign.getValue)
  }

  before {
    env = new MockEnv()
    executor = new FlowExecutorImpl(ConfigFactory.load(), ActorSystem("flow-test"), env)
    executor.start()
  }

  after {
    executor.stop()
    env = null
    executor = null
  }

  test("hello") {
    val event = newEvent("0", "a")("hello")
    whenReady(executor.execute(event)) { instance =>
      instance.assigns("result").value.as[String] shouldBe "hello world!"
    }
  }

  test("v2/hello") {
    val event = newEvent("0", "a")("v2/hello")
    whenReady(executor.execute(event)) { instance =>
      instance("procedure.length()").as[Int] shouldBe 1
      instance("procedure[0].flowName").as[String] shouldBe "/v2/hello"
      instance("procedure[0].assign.result").as[String] shouldBe "hello world!"
    }
  }

  test("composite") {
    val read = env.compositor("restful", "read")
    (read.composite _).expects(where { args: ValueDict =>
      args.visit("key").as[String] == "a"
    }).returns(Future.successful(Value(1)))
    (read.composite _).expects(where { args: ValueDict =>
      args.visit("key").as[String] == "b"
    }).returns(Future.successful(Value(2)))

    val write = env.compositor("restful", "write")
    (write.composite _).expects(where { args: ValueDict =>
      args.visit("output").as[Int] == 3
    }).returns(Future.successful(Value("success")))

    val event = newEvent("0", "a")("composite")
    whenReady(executor.execute(event)) { instance =>
      instance.assigns.keys should contain ("a")
      instance.assigns.keys should contain ("b")
      instance.assigns.keys should not contain ("c")
      instance.assigns("write").value.as[String] shouldBe "success"
    }
  }

  test("v2/composite") {
    val read = env.compositor("restful", "read")
    (read.composite _).expects(where { args: ValueDict =>
      args.visit("key").as[String] == "a"
    }).returns(Future.successful(Value(1)))
    (read.composite _).expects(where { args: ValueDict =>
      args.visit("key").as[String] == "b"
    }).returns(Future.successful(Value(2)))

    val write = env.compositor("restful", "write")
    (write.composite _).expects(where { args: ValueDict =>
      args.visit("output").as[Int] == 3
    }).returns(Future.successful(Value("success")))

    val event = newEvent("0", "a")("v2/composite")
    whenReady(executor.execute(event)) { instance =>
      instance("procedure[0].composite.length()").as[Int] shouldBe 3
      instance("procedure[0].composite.a.result").as[Int] shouldBe 1
      instance("procedure[0].composite.a.compositor").as[String] shouldBe "Read"
      instance("procedure[0].composite.b.result").as[Int] shouldBe 2
      instance("procedure[0].composite.b.argument.key").as[String] shouldBe "b"
      instance("procedure[0].composite.write.argument.output").as[Int] shouldBe 3
      instance("procedure[0].composite.write.compositor").as[String] shouldBe "Write"
    }
  }

  test("batch") {
    def concat(args: ValueDict): String = {
      args.visit("head").as[String] + args.visit("middle").as[String] + args.visit("last").toString
    }

    val compositor = env.compositor("concat", "")
    (compositor.composite _).expects(where { args: ValueDict => concat(args) == "a-1"}).returns(Future(Value("a-1")))
    (compositor.composite _).expects(where { args: ValueDict => concat(args) == "b-2"}).returns(Future(Value("b-2")))

    val event = newEvent("0", "a")("batch")
    whenReady(executor.execute(event)) { instance =>
      instance.assigns("result").value.as[Set[String]] shouldBe Set("a-1", "b-2")
    }
  }


  test("v2/batch") {
    def concat(args: ValueDict): String = {
      args.visit("head").as[String] + args.visit("middle").as[String] + args.visit("last").toString
    }

    val compositor = env.compositor("concat", "")
    (compositor.composite _).expects(where { args: ValueDict => concat(args) == "a-1"}).returns(Future(Value("a-1")))
    (compositor.composite _).expects(where { args: ValueDict => concat(args) == "b-2"}).returns(Future(Value("b-2")))

    val event = newEvent("0", "a")("v2/batch")
    whenReady(executor.execute(event)) { instance =>
      instance("procedure[0].composite.length()").as[Int] shouldBe 1
      instance("procedure[0].composite.result.compositor").as[String] shouldBe "Concat"
      instance("procedure[0].composite.result.batchSize").as[Int] shouldBe 2
      instance("procedure[0].composite.result.result").as[Map[String, String]] shouldBe Map("x" -> "a-1", "y" -> "b-2")
    }
  }

  test("branch") {
    def run(level: Int): Future[FlowInstance] = {
      val event = newEvent(level.toString, "a")("branch", "level" -> Value(level))
      executor.execute(event)
    }

    whenReady(run(1)) { instance =>
      instance.assigns("threshold").getChoice shouldBe "vip"
      instance.assigns("result").value.as[Double] shouldBe 0.5
      instance.chooses.keySet shouldBe Set("vip")
    }

    whenReady(run(2)) { instance =>
      instance.assigns("threshold").getChoice shouldBe "high"
      instance.assigns("result").value.as[Double] shouldBe 0.75
      instance.chooses.keySet shouldBe Set("normal", "high")
    }

    whenReady(run(3)) { instance =>
      instance.assigns("threshold").getChoice shouldBe "middle"
      instance.assigns("result").value.as[Double] shouldBe 0.75
      instance.chooses.keySet shouldBe Set("normal", "middle")
    }

    whenReady(run(4)) { instance =>
      instance.assigns("threshold").getChoice shouldBe "low"
      instance.assigns("result").value.as[Double] shouldBe 0.9
      instance.chooses.keySet shouldBe Set("normal", "low")

      instance.chooses("low").getPredicateList.map(_.getName).toSet shouldBe Set("middle", "high", "low")
      instance.assigns("low").getDependencyList.map(_.getName).toSet shouldBe Set("@level")
    }

    whenReady(run(5)) { instance =>
      instance.assigns.keySet should not contain "threshold"
      instance.assigns("result").value shouldBe NULL
      instance.chooses.keySet shouldBe Set("normal")
    }
  }

  test("v2/branch") {
    def run(level: Int): Future[FlowInstance] = {
      val event = newEvent(level.toString, "a")("v2/branch", "level" -> Value(level))
      executor.execute(event)
    }

    whenReady(run(1)) { instance =>
      instance("procedure[0].argument.level").as[Int] shouldBe 1
      instance("procedure[0].choice").as[Seq[String]] shouldBe Seq("vip")
      instance("procedure[0].assign.threshold").as[Double] shouldBe 0.5 +- 0.01
      instance("procedure[0].assign.result").as[Double] shouldBe 0.5 +- 0.01
    }

    whenReady(run(2)) { instance =>
      instance("procedure[0].choice").as[Seq[String]] shouldBe Seq("normal", "high")
      instance("procedure[0].assign.threshold").as[Double] shouldBe 0.75 +- 0.01
      instance("procedure[0].assign.result").as[Double] shouldBe 0.75 +- 0.01
      instance("procedure[0].assign.vip").as[Boolean] shouldBe false
      instance(""" "middle" not in procedure[0].assign """).as[Boolean] shouldBe true
    }

    whenReady(run(3)) { instance =>
      instance("procedure[0].choice").as[Seq[String]] shouldBe Seq("normal", "middle")
      instance("procedure[0].assign.threshold").as[Double] shouldBe 0.75 +- 0.01
      instance("procedure[0].assign.result").as[Double] shouldBe 0.75 +- 0.01
    }

    whenReady(run(4)) { instance =>
      instance("procedure[0].choice").as[Seq[String]] shouldBe Seq("normal", "low")
      instance("procedure[0].assign.threshold").as[Double] shouldBe 0.9 +- 0.01
      instance("procedure[0].assign.result").as[Double] shouldBe 0.9 +- 0.01
    }

    whenReady(run(5)) { instance =>
      instance("procedure[0].choice").as[Seq[String]] shouldBe Seq("normal")
      instance("procedure[0].assign.result") shouldBe NULL
      instance(""" "threshold" not in procedure[0].assign """).as[Boolean] shouldBe true
    }
  }

  test("trace") {
    env.updateContext("a", "0", "$delta", Value(1))

    var seq: Int = 0
    def run(flag: Boolean): Future[FlowInstance] = {
      (env.compositor("wait", "").composite _).expects(
        where {args: ValueDict => args.children.isEmpty}
      ).onCall({_: ValueDict => Future {
        Thread.sleep(200)
        Value(LocalDateTime.now().toString)
      }})

      seq += 1
      val event = newEvent(seq.toString, "a")("trace", "flag" -> Value(flag))
      executor.execute(event)
    }

    val one = run(false)
    val two = run(true)
    val three = run(false)
    val four = run(true)

    whenReady(one) { instance =>
      instance.assigns("$count").value.as[Int] shouldBe 0
      instance.assigns("result").value.as[Int] shouldBe 0
      instance.chooses.keySet should contain only "init"
    }

    whenReady(two) { instance =>
      instance.assigns("$count").value.as[Int] shouldBe 1
      instance.assigns("$count").getDependency(0).getEventId shouldBe "1"
      instance.assigns("$count").getDependency(0).getName shouldBe "$count"
      instance.assigns("result").value.as[Int] shouldBe 1
      instance.assigns("result").getDependency(0).getName shouldBe "$count"
      instance.assigns("result").getDependency(0).getEventId shouldBe "2"
      instance.chooses.keySet should contain only "add"
    }

    whenReady(three) { instance =>
      instance.assigns("result").value.as[Int] shouldBe 1
      instance.assigns("result").getDependency(0).getName shouldBe "$count"
      instance.assigns("result").getDependency(0).getEventId shouldBe "2"
      instance.chooses shouldBe empty
    }

    whenReady(four) { instance =>
      instance.assigns("result").value.as[Int] shouldBe 2
      instance.chooses.keySet should contain only "add"
    }
  }

  test("v2/trace") {
    env.updateContext("a", "0", "$delta", Value(1))

    var seq: Int = 0
    def run(flag: Boolean): Future[FlowInstance] = {
      (env.compositor("wait", "").composite _).expects(
        where {args: ValueDict => args.children.isEmpty}
      ).onCall({_: ValueDict => Future {
        Thread.sleep(200)
        Value(LocalDateTime.now().toString)
      }})

      seq += 1
      val event = newEvent(seq.toString, "a")("v2/trace", "flag" -> Value(flag))
      executor.execute(event)
    }

    val one = run(false)
    val two = run(true)
    val three = run(false)
    val four = run(true)

    whenReady(one) { instance =>
      instance("procedure[0].choice").as[Seq[String]] shouldBe Seq("init")
      instance("procedure[0].assign.result").as[Int] shouldBe 0
      instance("update[0].value").as[Int] shouldBe 0
      instance("update[0].reference.flowName").as[String] shouldBe "/v2/trace"
      instance("update[0].reference.name").as[String] shouldBe "$count"
      instance("update[0].reference.eventId").as[String] shouldBe "1"
    }

    whenReady(two) { instance =>
      instance("procedure[0].choice").as[Seq[String]] shouldBe Seq("add")
      instance("procedure[0].assign.result").as[Int] shouldBe 1
      instance("procedure[0].load[?(_.value == 0)].reference.eventId[-][0]").as[String] shouldBe "1"
      instance("update[?(_.value == 1)].reference.eventId[-][0]").as[String] shouldBe "2"
    }

    whenReady(three) { instance =>
      instance("procedure[0].choice") shouldBe NULL
      instance("procedure[0].assign.result").as[Int] shouldBe 1
      instance("procedure[0].load[?(_.value == 1)].reference.eventId[-][0]").as[String] shouldBe "2"
    }

    whenReady(four) { instance =>
      instance("procedure[0].choice").as[Seq[String]] shouldBe Seq("add")
      instance("procedure[0].assign.result").as[Int] shouldBe 2
      instance("procedure[0].load[?(_.value == 1)].reference.eventId[-][0]").as[String] shouldBe "2"
    }
  }

  test("error") {
    val action = env.compositor("action", "")
    (action.composite _).expects(
      where {_: ValueDict => true}
    ).returns(
      Future.failed(new NotImplementedError("hehe"))
    )

    val event = newEvent("0", "a")("error")
    whenReady(executor.execute(event)) { instance =>
      instance.assigns("action").value shouldBe NULL
      instance.assigns("error").value.visit("class").as[String] shouldBe "NotImplementedError"
      instance.assigns("error").value.visit("message").as[String] shouldBe "hehe"
    }
  }

  test("v2/failover") {
    val input = env.compositor("input", "")
    (input.composite _).expects(*).returns(
      Future.failed(new NotImplementedError("hehe"))
    )

    val action = env.compositor("action", "")
    (action.composite _).expects(*).never()

    val event = newEvent("0", "a")("v2/failover")
    whenReady(executor.execute(event)) { instance =>
      info(instance.toString)
      instance("procedure[0].choice").as[Seq[String]] shouldBe Seq("failover")
      instance("procedure[0].assign.result").as[Int] shouldBe 0
      instance("procedure[0].assign.failover").as[Boolean] shouldBe true
      instance(""" "success" not in procedure[0].assign """).as[Boolean] shouldBe true
      instance(""" "fail" not in procedure[0].assign """).as[Boolean] shouldBe true
      instance("procedure[0].fault.input.catalog").as[String] shouldBe "NotImplementedError"
      instance("procedure[0].fault.input.message").as[String] shouldBe "hehe"
      instance("procedure[0].fault.action.catalog").as[String] shouldBe "IllegalArgumentException"
    }
  }

  test("undefined-arg") {
    def run(level: Int): Future[FlowInstance] = {
      val event = newEvent(level.toString, "a")("undefined-arg", "level" -> Value(level))
      executor.execute(event)
    }

    whenReady(run(1)) { instance =>
      instance.assigns("arg").value shouldBe NULL
      instance.chooses.keySet shouldBe Set("is_null")
    }
  }

  test("goto") {
    val event = newEvent("1", "a")("goto", "arg" -> Value("argument"))
    whenReady(executor.execute(event)) { instance =>
      instance.assigns("need_infer").value.as[Boolean] shouldBe (true)
      instance.hasGoto should be (true)
    }
  }

  test("v2/schedule") {
    val event = newEvent("1", "a")("v2/schedule" )
    whenReady(executor.execute(event)) { instance =>
      instance("schedule[0].parent.eventId").as[String] shouldBe "1"
      instance("schedule[0].parent.traceId").as[String] shouldBe "a"
      instance("schedule[0].parent.flowName").as[String] shouldBe "/v2/schedule"

      instance("schedule[0].flowName").as[String] shouldBe "/v2/schedule"
      instance("schedule[0].traceId").as[String] shouldBe "a"
      instance("schedule[0].argument.x.value").as[Int] shouldBe 1
      instance("schedule[0].argument.y.value").as[Int] shouldBe 2

      instance("schedule[1].flowName").as[String] shouldBe "/v2/schedule"
      instance("schedule[1].traceId").as[String] shouldBe "b"
    }
  }

  test("v2/include") {
    val event = newEvent("1", "a")("v2/pi", "n" -> Value(32))
    whenReady(executor.execute(event)) { instance =>
      instance("procedure[0].assign.pi").as[Double] shouldBe 3.14159265 +- 0.00000001
      instance(""" procedure[?(_.flowName == "/v2/acrcot")].length() """).as[Int] shouldBe 66
      instance(""" procedure[?(_.flowName == "/v2/acrcot" and _.scope[0] == "left")].length() """).as[Int] shouldBe 33
      instance(""" procedure[?(_.flowName == "/v2/acrcot" and _.scope[0] == "right")].length() """).as[Int] shouldBe 33

      val lastProcedure =
        instance(""" procedure[?(_.flowName == "/v2/acrcot" and _.scope[0] == "left")][-][-1] """).as[ValueDict]
      lastProcedure.eval(""" scope[?(_ == "rest")].length() """).as[Int] shouldBe 32
    }
  }
}

