package com.databricks.apps.logs.chapter1

import java.util.concurrent.atomic.AtomicLong

import scala.math._

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import com.databricks.apps.logs.{ApacheAccessLog, OrderingUtils}

/**
 * This LogAnalyzerStreaming program reads the localhost 9999 socket
 * for incoming Apache access logs and computes the statistics on those logs
 * every 10 seconds.  It computes the statistics for data from all of time.
 *
 * To feed the new lines of some logfile into a socket, run this command:
 *   % tail -f [YOUR_LOG_FILE] | nc -lk 9999
 *
 * If you don't have a live log file that is being written to,
 * you can add test lines using this command:
 *   % cat data/apache.access.log >> [YOUR_LOG_FILE]
 *
 * Example command to run:
 * % spark-submit
 *   --class "com.databricks.apps.logs.chapter1.LogAnalyzerStreaming"
 *   --master local[4]
 *   target/scala-2.11/spark-logs-analyzer_2.11-2.0.jar
 */
object LogAnalyzerStreamingTotal extends App {
  val WINDOW_LENGTH = Seconds(30)
  val SLIDE_INTERVAL = Seconds(10)

  val computeRunningSum = (values: Seq[Long], state: Option[Long]) => {
    val currentCount = values.sum
    val previousCount = state.getOrElse(0L)
    Some(currentCount + previousCount)
  }

  val runningCount = new AtomicLong(0)
  val runningSum = new AtomicLong(0)
  val runningMin = new AtomicLong(Long.MaxValue)
  val runningMax = new AtomicLong(Long.MinValue)

  val sparkConf = new SparkConf().setAppName("Log Analyzer Streaming Total in Scala")

  val streamingContext = new StreamingContext(sparkConf, SLIDE_INTERVAL)

  // NOTE: Checkpointing must be enabled to use updateStateByKey.
  streamingContext.checkpoint("/tmp/log-analyzer-streaming-total-scala")

  val logLinesDStream = streamingContext.socketTextStream("localhost", 9999)

  val accessLogsDStream = logLinesDStream.map(ApacheAccessLog.parseLogLine).cache()

  // Calculate statistics based on the content size, and update variables to track this.
  val contentSizesDStream = accessLogsDStream.map(_.contentSize).cache()
  contentSizesDStream.foreachRDD(rdd => {
    val count = rdd.count()
    if (count > 0) {
      runningSum.getAndAdd(rdd.reduce(_ + _))
      runningCount.getAndAdd(count)
      runningMin.set(min(runningMin.get(), rdd.min()))
      runningMax.set(max(runningMax.get(), rdd.max()))
    }
    if (runningCount.get() == 0) {
      println("Content Size Avg: -, Min: -, Max: -")
    } else {
      println("Content Size Avg: %s, Min: %s, Max: %s".format(
        runningSum.get() / runningCount.get(),
        runningMin.get(),
        runningMax.get()
      ))
    }
  })

  // Compute Response Code to Count.
  // Note the use of updateStateByKey.
  val responseCodeCountDStream = accessLogsDStream
    .map(_.responseCode -> 1L)
    .reduceByKey(_ + _)
  val cumulativeResponseCodeCountDStream = responseCodeCountDStream
    .updateStateByKey(computeRunningSum)
  cumulativeResponseCodeCountDStream.foreachRDD(rdd => {
    val responseCodeToCount = rdd.take(100)
    println(s"""Response code counts: ${responseCodeToCount.mkString("[", ",", "]")}""")
  })

  // A DStream of ipAddresses accessed > 10 times.
  val ipAddressDStream = accessLogsDStream
    .map(_.ipAddress -> 1L)
    .reduceByKey(_ + _)
    .updateStateByKey(computeRunningSum)
    .filter(_._2 > 10)
    .map(_._1)
  ipAddressDStream.foreachRDD(rdd => {
    val ipAddresses = rdd.take(100)
    println(s"""IPAddresses > 10 times: ${ipAddresses.mkString("[", ",", "]")}""")
  })

  // A DStream of endpoint to count.
  val endpointCountsDStream = accessLogsDStream
    .map(_.endpoint -> 1L)
    .reduceByKey(_ + _)
    .updateStateByKey(computeRunningSum)
  endpointCountsDStream.foreachRDD(rdd => {
    val topEndpoints = rdd.top(10)(OrderingUtils.SecondValueLongOrdering)
    println(s"""Top Endpoints: ${topEndpoints.mkString("[", ",", "]")}""")
  })

  streamingContext.start()
  streamingContext.awaitTermination()
}
