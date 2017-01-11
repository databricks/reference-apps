package com.databricks.apps.logs.chapter1

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import com.databricks.apps.logs.ApacheAccessLog

/**
 * The LogAnalyzerStreamingSQL is similar to LogAnalyzerStreaming, except
 * it computes stats using Spark SQL.
 *
 * To feed the new lines of some logfile into a socket for streaming,
 * Run this command:
 *   % tail -f [YOUR_LOG_FILE] | nc -lk 9999
 *
 * If you don't have a live log file that is being written to,
 * you can add test lines using this command:
 *   % cat ../../data/apache.access.log >> [YOUR_LOG_FILE]
 *
 * Example command to run:
 * % spark-submit
 *   --class "com.databricks.apps.logs.chapter1.LogAnalyzerStreaming"
 *   --master local[*]
 *   target/scala-2.11/spark-logs-analyzer_2.11-2.0.jar
 *
 * On another console, run the shell script that emulates network stream
 * by periodically sending portions of the sample log file to a network socket:
 * % cd ../../data
 * % ./stream.sh apache.access.log
 */
object LogAnalyzerStreamingSQL extends App {
  val WINDOW_LENGTH = Seconds(30)
  val SLIDE_INTERVAL = Seconds(10)

  val spark = SparkSession
    .builder()
    .appName("Log Analyzer Streaming in Scala")
    .getOrCreate()
  import spark.implicits._
  val streamingContext = new StreamingContext(spark.sparkContext, SLIDE_INTERVAL)

  val logLinesDStream: DStream[String] = streamingContext.socketTextStream("localhost", 9999)

  val accessLogsDStream: DStream[ApacheAccessLog] = logLinesDStream.map(ApacheAccessLog.parseLogLine).cache()

  val windowDStream: DStream[ApacheAccessLog] = accessLogsDStream.window(WINDOW_LENGTH, SLIDE_INTERVAL)

  windowDStream.foreachRDD(accessLogs => {
    if (accessLogs.count() == 0) {
      println("No access logs received in this time interval")
    } else {
      accessLogs.toDF().createOrReplaceTempView("logs")

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
        .sql("SELECT responseCode, COUNT(*) FROM logs GROUP BY responseCode")
        .map(row => (row.getInt(0), row.getLong(1)))
        .take(1000)
      println(s"""Response code counts: ${responseCodeToCount.mkString("[", ",", "]")}""")

      // Any IPAddress that has accessed the server more than 10 times.
      val ipAddresses: Array[String] = spark
        .sql("SELECT ipAddress, COUNT(*) AS total FROM logs GROUP BY ipAddress HAVING total > 10")
        .map(row => row.getString(0))
        .take(100)
      println(s"""IPAddresses > 10 times: ${ipAddresses.mkString("[", ",", "]")}""")

      // Top Endpoints.
      val topEndpoints: Array[(String, Long)] = spark
        .sql("SELECT endpoint, COUNT(*) AS total FROM logs GROUP BY endpoint ORDER BY total DESC LIMIT 10")
        .map(row => (row.getString(0), row.getLong(1)))
        .collect()
      println(s"""Top Endpoints: ${topEndpoints.mkString("[", ",", "]")}""")
    }
  })

  // Start the streaming server.
  streamingContext.start() // Start the computation
  streamingContext.awaitTermination() // Wait for the computation to terminate
}
