package com.didichuxing.horoscope.ods.util

import java.util.Base64

import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowInstance
import com.didichuxing.horoscope.ods.ProcedureView
import com.didichuxing.horoscope.runtime.{NULL, Value, ValueDict}
import com.didichuxing.horoscope.util.Logging
import com.google.gson.{JsonObject, JsonParser}
import org.apache.spark.sql.SparkSession

object EtlUtil extends Logging {
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
    var v = Value(new JsonParser().parse(s)).as[ValueDict]
    if (v.visit("experiment") == NULL) {
      v = v.updated("experiment", Value(new JsonObject().toString))
    }
    if (v.visit("context_choice") == NULL) {
      v = v.updated("context_choice", Value(new JsonParser().parse("[]")))
    }
    v.as[ProcedureView]
  }

  def createSparkSession(name: String): SparkSession = {
    SparkSession.builder()
      .enableHiveSupport()
      .config("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")
      .config("spark.hive.mapred.supports.subdirectories", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")
      .appName(name).getOrCreate()
  }
}
