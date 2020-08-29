/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: liangguorong@didiglobal.com
 */

package com.didichuxing.horoscope.ods

import java.io.File
import java.util.Base64

import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowInstance
import com.didichuxing.horoscope.runtime.Value
import com.didichuxing.horoscope.util.Logging
import com.google.common.io.Resources
import com.google.gson.JsonParser
import com.google.protobuf.util.JsonFormat
import org.scalatest.{FunSuite, Matchers}

import scala.io.Source

class ProcedureViewSuite extends FunSuite with Matchers with Logging {

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

  test("extract procedure view") {
    val binary = loadData("flow_instance_base64.txt")
    val f = decodeAsFlowInstance(binary)
    info(s"json original size: ${JsonFormat.printer().omittingInsignificantWhitespace().print(f).size}")
    info(s"base 64 size: ${binary.length}")
    info(s"pb bytes size: ${f.toByteArray.size}")
    val builder = new ProcedureViewBuilder().withFlowInstance(f).withProcedure(f.getProcedure(0))
    val procedure = builder.build()
    info(procedure.toString)
  }

  test("procedure view") {
    val json = loadData("procedure_view_json.txt")
    val p = decodeAsProcedureView(json)
    info(p.toString)
  }

}
