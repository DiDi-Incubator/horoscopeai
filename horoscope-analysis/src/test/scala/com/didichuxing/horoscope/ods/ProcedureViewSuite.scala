/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: liangguorong@didiglobal.com
 */

package com.didichuxing.horoscope.ods

import java.io.File
import java.util.Base64

import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowInstance
import com.didichuxing.horoscope.runtime.Value
import com.didichuxing.horoscope.runtime.convert.ValueTypeAdapter
import com.didichuxing.horoscope.util.Logging
import com.google.common.io.Resources
import com.google.gson.{Gson, GsonBuilder, JsonParser}
import com.google.protobuf.util.JsonFormat
import org.scalatest.{FunSuite, Matchers}

import scala.io.Source

class ProcedureViewSuite extends FunSuite with Matchers with Logging {

  implicit val gson: Gson = new GsonBuilder()
    .registerTypeHierarchyAdapter(classOf[Value], new ValueTypeAdapter)
    .serializeNulls()
    .create()

  def loadData(file: String): String = {
    Source.fromFile(new File(Resources.getResource(s"data/${file}").getFile)).mkString
  }

  def decodeAsFlowInstance(s: String): FlowInstance = {
    try {
      FlowInstance.parseFrom(Base64.getDecoder.decode(s))
    } catch {
      case e: Exception =>
        logging.error(e.toString)
        FlowInstance.getDefaultInstance
    }
  }

  def decodeAsProcedureView(s: String): ProcedureView = {
    Value(new JsonParser().parse(s)).as[ProcedureView]
  }

  test("flow instance perf") {
    val binary = loadData("flow_instance_base64.txt")
    val f = decodeAsFlowInstance(binary)
    // json original size: 16713, json simple size: 6862, base 64 size: 9516, pb bytes size: 7137
    info(s"json original size: ${JsonFormat.printer().omittingInsignificantWhitespace().print(f).size}")
    info(s"json simple size: ${Value(f).toJson.size}") // json simple size: 6862
    info(s"base 64 size: ${binary.length}")
    info(s"pb bytes size: ${f.toByteArray.size}")
  }

  // scalastyle:off
  test("procedure view builder") {
    // this json is generated by run `ExecutorSuite` in horoscope-core
    val json = loadData("flow_instance_json.txt")
    val flowInstance = FlowInstance.newBuilder()
    JsonFormat.parser().ignoringUnknownFields().merge(json, flowInstance)
    val procedures = ProcedureViewBuilder.buildFrom(flowInstance.build())
    procedures.size should be > 0
    procedures.foreach { p =>
      println(Value(p).toJson)
    }
  }

  test("extract procedure view from json") {
    import Implicits._
    val json = loadData("procedure_view_json.txt")
    val procedureView = Value(new JsonParser().parse(json)).as[ProcedureView]
    info(procedureView.toJson().size.toString)
  }

}
