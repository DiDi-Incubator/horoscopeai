package com.didichuxing.horoscope.ods

import com.didichuxing.horoscope.runtime.{NULL, Value, ValueDict}
import com.google.common.io.Resources
import com.google.gson.{JsonObject, JsonParser}
import org.scalatest.{FunSuite, Matchers}

import java.io.File
import scala.io.Source

class ProcedureViewLoaderSuite extends FunSuite with Matchers {

  def loadData(file: String): String = {
    Source.fromFile(new File(Resources.getResource(s"data/${file}").getFile)).mkString
  }

  test("add with value") {
    val s: String = loadData("procedure_view_json.txt")
    var v = Value(new JsonParser().parse(s)).as[ValueDict]

    if (v.visit("experiment") == NULL) {
      v = v.updated("experiment", Value(new JsonObject().toString))
    }
    if (v.visit("context_choice") == NULL) {
      v = v.updated("context_choice", Value(new JsonParser().parse("[]")))
    }

    val p = v.as[ProcedureView]
    v.size shouldBe(16)
  }
}
