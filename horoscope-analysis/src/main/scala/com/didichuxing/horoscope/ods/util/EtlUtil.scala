/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: liangguorong@didiglobal.com
 */
package com.didichuxing.horoscope.ods.util
import java.util.Base64

import com.didichuxing.horoscope.core.FlowRuntimeMessage.FlowInstance
import com.didichuxing.horoscope.ods.ProcedureView
import com.didichuxing.horoscope.runtime.Value
import com.didichuxing.horoscope.util.Logging
import com.google.gson.JsonParser
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
    Value(new JsonParser().parse(s)).as[ProcedureView]
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
