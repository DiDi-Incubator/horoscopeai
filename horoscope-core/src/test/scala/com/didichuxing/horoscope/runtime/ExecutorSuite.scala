/*
 * Copyright (C) 2019 DiDi Inc. All Rights Reserved.
 * Authors: wenxiang@didiglobal.com
 * Description:
 */

package com.didichuxing.horoscope.runtime

import java.nio.charset.Charset
import java.time.LocalDateTime

import akka.actor.ActorSystem
import com.didichuxing.horoscope.core.FlowDslMessage.CompositorDef
import com.didichuxing.horoscope.core.FlowRuntimeMessage._
import com.didichuxing.horoscope.core.{Compositor, Flow}
import com.didichuxing.horoscope.dsl.{FlowCompiler}
import com.didichuxing.horoscope.runtime.experiment.{ExperimentController}
import com.google.common.io.Resources
import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.concurrent.Future
import scala.language.{implicitConversions, postfixOps}
import scala.util.Try

class ExecutorSuite extends ExecutorSuiteHelper {

  import com.didichuxing.horoscope.runtime.Implicits.{builtin, asDocument}
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.collection.JavaConversions._

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

    implicit def buildCompositor: (String, CompositorDef) => Compositor = { (flow, definition) =>
      compositor(definition.getFactory, definition.getContent)
    }

    override def getController(flow: String): Option[ExperimentController] = None

    override def getFlowByName(name: String): Flow = {
      flows.getOrElseUpdate(name, {
        val flow = Try {
          val code = Resources.toString(
            Resources.getResource(s"infoflow$name.flow"), Charset.defaultCharset()
          )
          val flowDef = FlowCompiler.compile(code)
          Flow(flowDef)
        }
        flow.get
      })
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
        |horoscope.flow-executor.log.detailed = true
        |""".stripMargin), ActorSystem("flow-test"), env)
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
      instance("procedure[0].flow_name").as[String] shouldBe "/v2/hello"
      instance("procedure[0].assign.result").as[String] shouldBe "hello world!"
      instance("procedure[0].assign.event_id").as[String] shouldBe "0"
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
      instance.assigns.keys should contain("a")
      instance.assigns.keys should contain("b")
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
      instance("procedure[0].assign.a").as[Int] shouldBe 1
      instance("procedure[0].composite.a.compositor").as[String] shouldBe "Read"
      instance("procedure[0].assign.b").as[Int] shouldBe 2
      //instance("procedure[0].composite.b.argument.key").as[String] shouldBe "b"
      //instance("procedure[0].composite.write.argument.output").as[Int] shouldBe 3
      instance("procedure[0].composite.write.compositor").as[String] shouldBe "Write"
    }
  }

  test("batch") {
    def concat(args: ValueDict): String = {
      args.visit("head").as[String] + args.visit("middle").as[String] + args.visit("last").toString
    }

    val compositor = env.compositor("concat", "")
    (compositor.composite _).expects(where { args: ValueDict => concat(args) == "a-1" }).returns(Future(Value("a-1")))
    (compositor.composite _).expects(where { args: ValueDict => concat(args) == "b-2" }).returns(Future(Value("b-2")))

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
    (compositor.composite _).expects(where { args: ValueDict => concat(args) == "a-1" }).returns(Future(Value("a-1")))
    (compositor.composite _).expects(where { args: ValueDict => concat(args) == "b-2" }).returns(Future(Value("b-2")))

    val event = newEvent("0", "a")("v2/batch")
    whenReady(executor.execute(event)) { instance =>
      instance("procedure[0].composite.length()").as[Int] shouldBe 1
      instance("procedure[0].composite.result.compositor").as[String] shouldBe "Concat"
      instance("procedure[0].composite.result.batch_size").as[Int] shouldBe 2
      instance("procedure[0].assign.result").as[Map[String, String]] shouldBe Map("x" -> "a-1", "y" -> "b-2")
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
        where { args: ValueDict => args.children.isEmpty }
      ).onCall({ _: ValueDict =>
        Future {
          Thread.sleep(200)
          Value(LocalDateTime.now().toString)
        }
      })

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
        where { args: ValueDict => args.children.isEmpty }
      ).onCall({ _: ValueDict =>
        Future {
          Thread.sleep(200)
          Value(LocalDateTime.now().toString)
        }
      })

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
      instance("update[0].reference.flow_name").as[String] shouldBe "/v2/trace"
      instance("update[0].reference.name").as[String] shouldBe "$count"
      instance("update[0].reference.event_id").as[String] shouldBe "1"
    }

    whenReady(two) { instance =>
      instance("procedure[0].choice").as[Seq[String]] shouldBe Seq("add")
      instance("procedure[0].assign.result").as[Int] shouldBe 1
      instance("procedure[0].load[?(_.value == 0)].reference.event_id[-][0]").as[String] shouldBe "1"
      instance("update[?(_.value == 1)].reference.event_id[-][0]").as[String] shouldBe "2"
    }

    whenReady(three) { instance =>
      instance("procedure[0].choice") shouldBe NULL
      instance("procedure[0].assign.result").as[Int] shouldBe 1
      instance("procedure[0].load[?(_.value == 1)].reference.event_id[-][0]").as[String] shouldBe "2"
    }

    whenReady(four) { instance =>
      instance("procedure[0].choice").as[Seq[String]] shouldBe Seq("add")
      instance("procedure[0].assign.result").as[Int] shouldBe 2
      instance("procedure[0].load[?(_.value == 1)].reference.event_id[-][0]").as[String] shouldBe "2"
    }
  }

  test("error") {
    val action = env.compositor("action", "")
    (action.composite _).expects(
      where { _: ValueDict => true }
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
      instance.hasGoto shouldBe true
    }
  }

  test("v2/schedule") {
    val event = newEvent("1", "a")("v2/schedule")
    whenReady(executor.execute(event)) { instance =>
      instance("schedule[0].parent.event_id").as[String] shouldBe "1"
      instance("schedule[0].parent.trace_id").as[String] shouldBe "a"
      instance("schedule[0].parent.flow_name").as[String] shouldBe "/v2/schedule"

      instance("schedule[0].flow_name").as[String] shouldBe "/v2/schedule"
      instance("schedule[0].trace_id").as[String] shouldBe "a"
      instance("schedule[0].argument.x.value").as[Int] shouldBe 1
      instance("schedule[0].argument.y.value").as[Int] shouldBe 2

      instance("schedule[1].flow_name").as[String] shouldBe "/v2/schedule"
      instance("schedule[1].trace_id").as[String] shouldBe "b"
    }
  }

  test("v2/include") {
    val event = newEvent("1", "a")("v2/pi", "n" -> Value(32))
    whenReady(executor.execute(event)) { instance =>
      instance("procedure[0].assign.pi").as[Double] shouldBe 3.14159265 +- 0.00000001
      instance(""" procedure[?(_.flow_name == "/v2/acrcot")].length() """).as[Int] shouldBe 66
      instance(""" procedure[?(_.flow_name == "/v2/acrcot" and _.scope[1] == "left")].length() """).as[Int] shouldBe 33
      instance(""" procedure[?(_.flow_name == "/v2/acrcot" and _.scope[1] == "right")].length() """).as[Int] shouldBe 33

      val lastProcedure =
        instance(""" procedure[?(_.flow_name == "/v2/acrcot" and _.scope[1] == "left")][-][-1] """).as[ValueDict]
      lastProcedure.eval(""" scope[?(_ == "rest")].length() """).as[Int] shouldBe 32
    }
  }

  test("v2/transient") {
    var seq: Int = 0

    def run(delta: Option[Int]): Future[FlowInstance] = {
      seq += 1
      val event = newEvent(seq.toString, "a")("v2/transient", delta.map(d => "input_delta" -> Value(d)).toArray: _*)
      executor.execute(event)
    }

    val one = run(None)
    val two = run(Some(1))
    val five = run(Some(3))
    val ten = run(Some(5))

    whenReady(one) { instance =>
      instance("procedure[0].assign").as[Map[String, Int]] shouldBe Map("result" -> 1)
      instance.getUpdateList.isEmpty shouldBe true
    }

    whenReady(two) { instance =>
      instance("procedure[0].assign").as[Map[String, Int]] shouldBe Map("result" -> 2)
      instance.getUpdateList.isEmpty shouldBe true
    }

    whenReady(five) { instance =>
      instance("procedure[0].assign").as[Map[String, Int]] shouldBe Map("result" -> 5)
      instance.getUpdateList.isEmpty shouldBe true
    }

    whenReady(ten) { instance =>
      instance("procedure[0].assign").as[Map[String, Int]] shouldBe Map("result" -> 10)
      instance.getUpdateList.isEmpty shouldBe true
    }
  }

}

