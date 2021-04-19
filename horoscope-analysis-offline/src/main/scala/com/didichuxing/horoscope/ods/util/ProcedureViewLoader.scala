/*
 * Copyright (C) 2020 DiDi Inc. All Rights Reserved.
 * Authors: liangguorong@didiglobal.com
 */

package com.didichuxing.horoscope.ods.util
import com.didichuxing.horoscope.ods.ProcedureView
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * Kafka Pb Bytes => Flink APP => Kafka Json => HDFS => ProcedureViewLoader => Hive Table
 * Extract from json, then save as ORC Hive Table
 */
class ProcedureViewLoader(
  @transient val sparkSession: SparkSession,
  basePath: String,
  targetTable: String) extends Serializable {
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
    println(s"Input lines ${inputCount}")
    val transformed = input.rdd.filter{r =>
      val str = r.getAs[String](0).trim
      str.length > 0 && str.startsWith("{") && str.endsWith("}")
    }.map{ r =>
      try {
        EtlUtil.decodeAsProcedureView(r.getAs[String](0).trim)
      } catch {
        case e: Throwable =>
          print(e.toString)
          ProcedureView()
      }
    }.filter(v => v.trace_id.length > 0 && v.flow_name.length > 0)
    transformed.persist(StorageLevel.MEMORY_AND_DISK)
    val validCount = transformed.count()
    println(s"Input lines ${inputCount}, transformed valid ${validCount}")
    if (validCount > 0) {
      // select columns the same as hive table schema: traffic_online.ods_roadnet_procedure_view_log_online
      val output = sparkSession.createDataset(transformed)
        .select("trace_id", "id",
          "flow_name", "start_time", "end_time", "choice", "argument",
          "result", "composite", "fault", "flow_id", "load", "ancestor",
          "descendants", "experiment", "context_choice")
        .withColumn("year", lit(year))
        .withColumn("month", lit(month))
        .withColumn("day", lit(day))
        .withColumn("hour", lit(hour))
      output.printSchema()
      output.write.mode("overwrite").insertInto(targetTable)
      sparkSession.sql(s"ALTER TABLE $targetTable PARTITION (year=$year, month=$month, day=$day, hour=$hour) CONCATENATE") // concatenate small files into large files
      println(s"Input lines ${inputCount}, transformed valid ${validCount}, " +
        s"output rows ${output.count()}, time taken ${System.currentTimeMillis() - startTime} ms")
    } else {
      println(s"Input ${inputPath} is empty")
    }
  }
}

object ProcedureViewLoader {
  def run(basePath: String, targetTable: String, dateTime: String): Unit = {
    println(s"Prepared env, basePath ${basePath}, dateTime ${dateTime}")
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
    run(basePath, targetTable, dateTime)
  }
}
