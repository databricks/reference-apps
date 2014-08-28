package com.databricks.apps.logs.chapter1

import com.databricks.apps.logs.{ApacheAccessLog, OrderingUtils}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{StreamingContext, Duration}
import org.apache.spark.SparkContext._

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
 *   --master local[4]
 *   target/scala-2.10/spark-logs-analyzer_2.10-1.0.jar
 */
object LogAnalyzerStreaming {
  val WINDOW_LENGTH = new Duration(30 * 1000)
  val SLIDE_INTERVAL = new Duration(10 * 1000)

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Log Analyzer Streaming in Scala")
    val sc = new SparkContext(sparkConf)
    val streamingContext = new StreamingContext(sc, SLIDE_INTERVAL)

    val logLinesDStream = streamingContext.socketTextStream("localhost", 9999)

    val accessLogsDStream = logLinesDStream.map(ApacheAccessLog.parseLogLine).cache()
    val windowDStream = accessLogsDStream.window(WINDOW_LENGTH, SLIDE_INTERVAL)

    windowDStream.foreachRDD(accessLogs => {
      if (accessLogs.count() == 0) {
        println("No access com.databricks.app.logs received in this time interval")
      } else {
        // Calculate statistics based on the content size.
        val contentSizes = accessLogs.map(log => log.contentSize).cache()
        println("Content Size Avg: %s, Min: %s, Max: %s".format(
          contentSizes.reduce(_ + _) / contentSizes.count,
          contentSizes.min,
          contentSizes.max
        ))

        // Compute Response Code to Count.
        val responseCodeToCount = accessLogs
          .map(log => (log.responseCode, 1))
          .reduceByKey(_ + _)
          .take(100)
        println( s"""Response code counts: ${responseCodeToCount.mkString("[", ",", "]")}""")

        // Any IPAddress that has accessed the server more than 10 times.
        val ipAddresses = accessLogs
          .map(log => (log.ipAddress, 1))
          .reduceByKey(_ + _)
          .filter(_._2 > 10)
          .map(_._1)
          .take(100)
        println( s"""IPAddresses > 10 times: ${ipAddresses.mkString("[", ",", "]")}""")

        // Top Endpoints.
        val topEndpoints = accessLogs
          .map(log => (log.endpoint, 1))
          .reduceByKey(_ + _)
          .top(10)(OrderingUtils.SecondValueOrdering)
        println( s"""Top Endpoints: ${topEndpoints.mkString("[", ",", "]")}""")
      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
