package com.databricks.apps.logs.chapter3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import com.databricks.apps.logs.ApacheAccessLog

/**
  * LogAnalyzerExportRDD shows how to export data a set of files.
  *
  * Example command to run:
  * %  ${YOUR_SPARK_HOME}/bin/spark-submit
  * --class "com.databricks.apps.logs.chapter3.LogAnalyzerExportRDD"
  * --master spark://YOUR_SPARK_MASTER
  * target/scala-2.11/spark-logs-analyzer_2.11-2.0.jar
  * ../../data/apache.access.log output_dir
  */
object LogAnalyzerExportRDD extends App {
  // Optionally modify this based as makes sense for your dataset.
  val NUM_PARTITIONS = 2

  // Create the spark context.
  val sparkConf = new SparkConf().setAppName("Log Analyzer in Scala")
  val sc = new SparkContext(sparkConf)

  val inputFile = args(0)
  val outputDirectory = args(1)

  val accessLogs: RDD[ApacheAccessLog] = sc.textFile(inputFile)
    .map(ApacheAccessLog.parseLogLine)
    .repartition(NUM_PARTITIONS) // Optionally, change this.
  accessLogs.saveAsTextFile(outputDirectory)

  sc.stop()
}
