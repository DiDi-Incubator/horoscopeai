package com.didichuxing.horoscope.runtime

import com.didichuxing.horoscope.core.FlowRuntimeMessage.{FlowEvent, FlowInstance, FlowValue, TraceVariable}
import com.didichuxing.horoscope.runtime.convert.ValueTypeAdapter
import com.didichuxing.horoscope.util.Logging
import com.google.gson.{Gson, GsonBuilder}
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

abstract class ExecutorSuiteHelper extends FunSuite
  with Matchers with ScalaFutures
  with MockFactory with BeforeAndAfter
  with Logging {

  import com.didichuxing.horoscope.runtime.Implicits.builtin

  import scala.collection.JavaConversions._

  implicit val gson: Gson = new GsonBuilder()
    .registerTypeHierarchyAdapter(classOf[Value], new ValueTypeAdapter)
    .setPrettyPrinting()
    .create()

  implicit val defaultPatience: PatienceConfig =
    PatienceConfig(timeout = Span(10000, Seconds), interval = Span(5, Millis))

  val emptyDict: ValueDict = Value(Map.empty[String, Value])

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

    def apply(code: String): Value = {
      Value(instance).asInstanceOf[ValueDict].eval(code)
    }

    def toJson: String = Value(instance).toJson
  }

}
