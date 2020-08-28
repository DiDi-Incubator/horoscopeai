/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: liangguorong@didiglobal.com
 */

package com.didichuxing.horoscope.ods.util

import com.didichuxing.horoscope.util.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * Kafka Pb Bytes => Flink APP => Kafka Json => HDFS => ProcedureViewLoader => Hive Table
 * Extract from json
 */
class ProcedureViewLoader(
  @transient val sparkSession: SparkSession,
  basePath: String,
  targetTable: String) extends Logging with Serializable {
  import org.apache.spark.sql.functions._
  import sparkSession.implicits._

  // scalastyle:off
  def run(year: String, month: String, day: String, hour: String): Unit = {
    val startTime = System.currentTimeMillis()
    val inputPath = s"${basePath}/${year}/${month}/${day}/${hour}"
    println(s"Starting to extract ${inputPath}")
    val input = sparkSession.read.format("text").load(inputPath)
    input.persist(StorageLevel.MEMORY_AND_DISK)
    val inputCount = input.count()
    println(s"Input count ${inputCount}")
    val transformed = input.rdd.filter(_.getAs[String](0).length > 0)
      .map { r => EtlUtil.decodeAsProcedureView(r.getAs[String](0))}
    if (inputCount > 0) {
      val output = sparkSession.createDataset(transformed)
        .withColumn("year", lit(year))
        .withColumn("month", lit(month))
        .withColumn("day", lit(day))
        .withColumn("hour", lit(hour))
      output.printSchema()
      output.write.insertInto(targetTable)
      println(s"Output rows ${output.count()}, time taken ${System.currentTimeMillis() - startTime} ms")
    } else {
      println("Input is empty")
    }

  }
}

object ProcedureViewLoader extends Logging {
  def run(basePath: String, targetTable: String, dateTime: String, offsetHour: Int): Unit = {
    val dateRanges = DateUtil.dateRange(dateTime, offsetHour)
    println(s"Prepared env, basePath ${basePath}, date ranges: ${dateRanges.mkString("[", ",", "]")}")
    val spark = EtlUtil.createSparkSession(this.getClass.getCanonicalName)
    val extractor = new ProcedureViewLoader(spark, basePath, targetTable)
    val year = dateTime.substring(0, 4)
    val month = dateTime.substring(4, 6)
    val day = dateTime.substring(6, 8)
    val hour = dateTime.substring(8, 10)
    extractor.run(year, month, day, hour)
  }

  def printUsage(): Unit = {
    println(s"""
      |./${this.getClass.getCanonicalName} --basePath basePath --targetTable targetTable --dateTime dateTime(2019011423)
      |""".stripMargin)
  }

  def main(args: Array[String]): Unit = {
    var basePath: String = ""
    var targetTable: String = ""
    var dateTime: String = ""
    var offsetHour: Int = -2
    args.sliding(2).foreach {
      case Array("--basePath", p) =>
        basePath = p
      case Array("--targetTable", t) =>
        targetTable = t
      case Array("--dateTime", d) =>
        dateTime = d
      case _ =>
    }
    run(basePath, targetTable, dateTime, offsetHour)
  }
}
