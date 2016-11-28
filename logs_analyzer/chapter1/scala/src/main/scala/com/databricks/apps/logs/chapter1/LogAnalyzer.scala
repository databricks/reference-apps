package com.databricks.apps.logs.chapter1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import com.databricks.apps.logs.ApacheAccessLog

/**
 * The LogAnalyzer takes in an apache access log file and
 * computes some statistics on them.
 *
 * Example command to run:
 * % spark-submit
 *   --class "com.databricks.apps.logs.chapter1.LogAnalyzer"
 *   --master local[*]
 *   target/scala-2.11/spark-logs-analyzer_2.11-2.0.jar
 *   ../../data/apache.access.log
 */
object LogAnalyzer extends App {
  val sparkConf = new SparkConf().setAppName("Log Analyzer in Scala")
  val sc = new SparkContext(sparkConf)

  val logFile = args(0)

  val accessLogs: RDD[ApacheAccessLog] = sc.
    textFile(logFile).
    map(ApacheAccessLog.parseLogLine).cache()

  // Calculate statistics based on the content size.
  val contentSizes: RDD[Long] = accessLogs.map(_.contentSize).cache()
  println("Content Size Avg: %s, Min: %s, Max: %s".format(
    contentSizes.reduce(_ + _) / contentSizes.count,
    contentSizes.min,
    contentSizes.max))

  // Compute Response Code to Count.
  val responseCodeToCount: Array[(Int, Long)] = accessLogs
    .map(_.responseCode -> 1L)
    .reduceByKey(_ + _)
    .take(100)
  println(s"""Response code counts: ${responseCodeToCount.mkString("[", ",", "]")}""")

  // Any IPAddress that has accessed the server more than 10 times.
  val ipAddresses: Array[String] = accessLogs
    .map(_.ipAddress -> 1L)
    .reduceByKey(_ + _)
    .filter(_._2 > 10)
    .map(_._1)
    .take(100)
  println(s"""IPAddresses > 10 times: ${ipAddresses.mkString("[", ",", "]")}""")

  // Top Endpoints.
  val topEndpoints: Array[(String, Long)] = accessLogs
    .map(_.endpoint -> 1L)
    .reduceByKey(_ + _)
    .top(10)(Ordering.by[(String, Long), Long](_._2))
  println(s"""Top Endpoints: ${topEndpoints.mkString("[", ",", "]")}""")

  sc.stop()
}
