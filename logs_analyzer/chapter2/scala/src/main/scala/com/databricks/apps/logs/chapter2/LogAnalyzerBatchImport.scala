package com.databricks.apps.logs.chapter2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import com.databricks.apps.logs.{ApacheAccessLog, LogAnalyzerRDD}

/**
  * LogAnalyzerBatchImport shows how to import data from a file.
  *
  * Example command to run:
  * %  ${YOUR_SPARK_HOME}/bin/spark-submit
  * --class "com.databricks.apps.logs.chapter2.LogAnalyzerBatchImport"
  * --master spark://YOUR_SPARK_MASTER
  * target/scala-2.11/spark-logs-analyzer_2.11-2.0.jar
  * ../../data/apache.access.log
  */
object LogAnalyzerBatchImport extends App {
  val spark = SparkSession
    .builder()
    .appName("Log Analyzer SQL in Scala")
    .getOrCreate()

  val logFile = args(0)

  val accessLogs: RDD[ApacheAccessLog] = spark.sparkContext
    .textFile(logFile)
    .map(ApacheAccessLog.parseLogLine)

  val logAnalyzerRDD = LogAnalyzerRDD(spark)
  val logStatistics = logAnalyzerRDD.processRdd(accessLogs)
  logStatistics.printToStandardOut()

  spark.stop()
}
