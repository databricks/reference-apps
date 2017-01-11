package com.databricks.apps.logs.chapter3

import java.io.{BufferedWriter, FileWriter, PrintWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import com.databricks.apps.logs.{ApacheAccessLog, LogAnalyzerRDD}

/**
  * LogAnalyzerExportSmallData shows how to export data of small size to a file.
  *
  * Example command to run:
  * %  ${YOUR_SPARK_HOME}/bin/spark-submit
  * --class "com.databricks.apps.logs.chapter3.LogAnalyzerExportSmallData"
  * --master spark://YOUR_SPARK_MASTER
  * target/scala-2.11/spark-logs-analyzer_2.11-2.0.jar
  * ../../data/apache.access.log output.log
  */
object LogAnalyzerExportSmallData extends App {
  // Initialize SparkSession instance.
  val spark = SparkSession
    .builder()
    .appName("Log Analyzer SQL in Scala")
    .getOrCreate()

  val logFile = args(0)

  val accessLogs: RDD[ApacheAccessLog] = spark
    .sparkContext
    .textFile(logFile)
    .map(ApacheAccessLog.parseLogLine)

  val logAnalyzerRDD = LogAnalyzerRDD(spark)
  val logStatistics = logAnalyzerRDD.processRdd(accessLogs)

  val outputFile = args(1)

  val out = new PrintWriter(new BufferedWriter(new FileWriter(outputFile)))
  val contentSizeStats = logStatistics.contentSizeStats
  out.println("Content Size Avg: %s, Min: %s, Max: %s"
    .format(contentSizeStats._1 / contentSizeStats._2,
      contentSizeStats._3,
      contentSizeStats._4))
  out.println(s"""Response code counts: ${logStatistics.responseCodeToCount.mkString("[", ",", "]")}""")
  out.println(s"""IPAddresses > 10 times: ${logStatistics.ipAddresses.mkString("[", ",", "]")}""")
  out.println(s"""Top Endpoints: ${logStatistics.topEndpoints.mkString("[", ",", "]")}""")
  out.close()

  spark.stop()
}
