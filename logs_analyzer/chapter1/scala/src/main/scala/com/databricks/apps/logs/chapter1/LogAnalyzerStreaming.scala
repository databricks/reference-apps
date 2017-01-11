package com.databricks.apps.logs.chapter1

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import com.databricks.apps.logs.ApacheAccessLog

/**
 * The LogAnalyzerStreaming illustrates how to use logs with Spark Streaming to
 *   compute statistics every slide_interval for the last window length of time.
 *
 * To feed the new lines of some logfile into a socket, run this command:
 *   % tail -f [YOUR_LOG_FILE] | nc -lk 9999
 *
 * If you don't have a live log file that is being written to, you can add test lines using this command:
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
object LogAnalyzerStreaming extends App {
  val WINDOW_LENGTH = Seconds(30)
  val SLIDE_INTERVAL = Seconds(10)

  val sparkConf = new SparkConf().setAppName("Log Analyzer Streaming in Scala")
  val streamingContext = new StreamingContext(sparkConf, SLIDE_INTERVAL)

  val logLinesDStream: DStream[String] = streamingContext.socketTextStream("localhost", 9999)

  val accessLogsDStream: DStream[ApacheAccessLog] = logLinesDStream.map(ApacheAccessLog.parseLogLine).cache()
  val windowDStream: DStream[ApacheAccessLog] = accessLogsDStream.window(WINDOW_LENGTH, SLIDE_INTERVAL)

  windowDStream.foreachRDD(accessLogs => {
    if (accessLogs.count() == 0) {
      println("No access logs received in this time interval")
    } else {
      // Calculate statistics based on the content size.
      val contentSizes: RDD[Long] = accessLogs.map(_.contentSize).cache()
      println("Content Size Avg: %s, Min: %s, Max: %s".format(
        contentSizes.reduce(_ + _) / contentSizes.count,
        contentSizes.min,
        contentSizes.max
      ))

      // Compute Response Code to Count.
      val responseCodeToCount: Array[(Int, Long)] = accessLogs
        .map(_.responseCode -> 1L)
        .reduceByKey(_ + _)
        .take(100)
      println( s"""Response code counts: ${responseCodeToCount.mkString("[", ",", "]")}""")

      // Any IPAddress that has accessed the server more than 10 times.
      val ipAddresses: Array[String] = accessLogs
        .map(_.ipAddress -> 1L)
        .reduceByKey(_ + _)
        .filter(_._2 > 10)
        .map(_._1)
        .take(100)
      println( s"""IPAddresses > 10 times: ${ipAddresses.mkString("[", ",", "]")}""")

      // Top Endpoints.
      val topEndpoints: Array[(String, Long)] = accessLogs
        .map(_.endpoint -> 1L)
        .reduceByKey(_ + _)
        .top(10)(Ordering.by[(String, Long), Long](_._2))
      println( s"""Top Endpoints: ${topEndpoints.mkString("[", ",", "]")}""")
    }
  })

  // Start the streaming server.
  streamingContext.start() // Start the computation
  streamingContext.awaitTermination() // Wait for the computation to terminate
}
