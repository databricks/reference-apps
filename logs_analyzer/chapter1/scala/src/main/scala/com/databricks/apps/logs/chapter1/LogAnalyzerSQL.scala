package com.databricks.apps.logs.chapter1

import org.apache.spark.sql.{Dataset, Row, SparkSession}

import com.databricks.apps.logs.ApacheAccessLog

/**
 * LogAnalyzerSQL shows how to use SQL syntax with Spark.
 *
 * Example command to run:
 * % spark-submit
 *   --class "com.databricks.apps.logs.chapter1.LogAnalyzerSQL"
 *   --master local[*]
 *   target/scala-2.11/spark-logs-analyzer_2.11-2.0.jar
 *   ../../data/apache.access.log
 */
object LogAnalyzerSQL extends App {
  val spark = SparkSession
    .builder()
    .appName("Log Analyzer SQL in Scala")
    .getOrCreate()
  import spark.implicits._

  val logFile = args(0)

  // Read Dataset of lines from the file
  val accessLogs: Dataset[ApacheAccessLog] = spark
    .read
    .textFile(logFile)
    .map(ApacheAccessLog.parseLogLine)

  // Register the Dataset as a temporary view.
  accessLogs.createOrReplaceTempView("logs")

  // Calculate statistics based on the content size.
  val contentSizeStats: Row = spark
    .sql("SELECT SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize) FROM logs")
    .first()
  println("Content Size Avg: %s, Min: %s, Max: %s".format(
    contentSizeStats.getLong(0) / contentSizeStats.getLong(1),
    contentSizeStats(2),
    contentSizeStats(3)))

  // Compute Response Code to Count.
  val responseCodeToCount: Array[(Int, Long)] = spark
    .sql("SELECT responseCode, COUNT(*) FROM logs GROUP BY responseCode LIMIT 1000")
    .map(row => (row.getInt(0), row.getLong(1)))
    .collect()
  println(s"""Response code counts: ${responseCodeToCount.mkString("[", ",", "]")}""")

  // Any IPAddress that has accessed the server more than 10 times.
  val ipAddresses: Array[String] =spark
    .sql("SELECT ipAddress, COUNT(*) AS total FROM logs GROUP BY ipAddress HAVING total > 10 LIMIT 1000")
    .map(row => row.getString(0))
    .collect()
  println(s"""IPAddresses > 10 times: ${ipAddresses.mkString("[", ",", "]")}""")

  // Top Endpoints
  val topEndpoints: Array[(String, Long)] = spark
    .sql("SELECT endpoint, COUNT(*) AS total FROM logs GROUP BY endpoint ORDER BY total DESC LIMIT 10")
    .map(row => (row.getString(0), row.getLong(1)))
    .collect()
  println(s"""Top Endpoints: ${topEndpoints.mkString("[", ",", "]")}""")

  spark.stop()
}
