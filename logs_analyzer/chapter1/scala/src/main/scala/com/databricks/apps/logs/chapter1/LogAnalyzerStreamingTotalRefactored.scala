package com.databricks.apps.logs.chapter1

import java.util.concurrent.atomic.AtomicLong

import scala.math._

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import com.databricks.apps.logs.ApacheAccessLog

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
 *   --master local[*]
 *   target/scala-2.11/spark-logs-analyzer_2.11-2.0.jar
 *
 * On another console, run the shell script that emulates network stream
 * by periodically sending portions of the sample log file to a network socket:
 * % cd ../../data
 * % ./stream.sh apache.access.log
 */
object LogAnalyzerStreamingTotalRefactored extends App {
  val SLIDE_INTERVAL = Seconds(10)

  val computeRunningSum: (Seq[Long], Option[Long]) => Option[Long]
  = (values: Seq[Long], state: Option[Long]) => {
    val currentCount = values.sum
    val previousCount = state.getOrElse(0L)
    Some(currentCount + previousCount)
  }

  // These static variables store the running content size values.
  val runningCount = new AtomicLong(0)
  val runningSum = new AtomicLong(0)
  val runningMin = new AtomicLong(Long.MaxValue)
  val runningMax = new AtomicLong(Long.MinValue)

  // These functions below that could be shared with the batch library.
  val contentSizeStats: RDD[ApacheAccessLog] => (Long, Long, Long, Long)
  = (accessLogRDD: RDD[ApacheAccessLog]) => {
    val contentSizes = accessLogRDD.map(_.contentSize).cache()
    (contentSizes.count(), contentSizes.reduce(_ + _),
      contentSizes.min, contentSizes.max)
  }

  val responseCodeCount: RDD[ApacheAccessLog] => RDD[(Int, Long)]
  = (accessLogRDD: RDD[ApacheAccessLog]) =>
    accessLogRDD.map(_.responseCode -> 1L).reduceByKey(_ + _)

  val ipAddressCount: RDD[ApacheAccessLog] => RDD[(String, Long)]
  = (accessLogRDD: RDD[ApacheAccessLog]) =>
    accessLogRDD.map(_.ipAddress -> 1L).reduceByKey(_ + _)

  val filterIPAddress: RDD[(String, Long)] => RDD[String]
  = (ipAddressCount: RDD[(String, Long)]) =>
    ipAddressCount.filter(_._2 > 10).map(_._1)

  val endpointCount: RDD[ApacheAccessLog] => RDD[(String, Long)]
  = (accessLogRDD: RDD[ApacheAccessLog]) =>
    accessLogRDD.map(_.endpoint -> 1L).reduceByKey(_ + _)

  val sparkConf = new SparkConf().setAppName("Log Analyzer Streaming Total in Scala")

  val streamingContext = new StreamingContext(sparkConf, SLIDE_INTERVAL)

  // NOTE: Checkpointing must be enabled to use updateStateByKey.
  streamingContext.checkpoint("checkpoints-log-analyzer-streaming-total")

  val logLinesDStream: DStream[String] = streamingContext.socketTextStream("localhost", 9999)

  val accessLogsDStream: DStream[ApacheAccessLog] = logLinesDStream.map(ApacheAccessLog.parseLogLine).cache()

  // Calculate statistics based on the content size.
  accessLogsDStream.foreachRDD(rdd => {
    val count = rdd.count()
    if (count > 0) {
      val currentContentSizes: (Long, Long, Long, Long) = contentSizeStats(rdd)
      runningCount.getAndAdd(currentContentSizes._1)
      runningSum.getAndAdd(currentContentSizes._2)
      runningMin.set(min(runningMin.get(), currentContentSizes._3))
      runningMax.set(max(runningMax.get(), currentContentSizes._4))
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
  // Notice the use of transform to produce the a DStream of
  // response code counts, and then updateStateByKey to accumulate
  // the response code counts for all of time.
  val responseCodeCountDStream: DStream[(Int, Long)] = accessLogsDStream
    .transform(responseCodeCount)
  val cumulativeResponseCodeCountDStream: DStream[(Int, Long)] = responseCodeCountDStream
    .updateStateByKey(computeRunningSum)
  cumulativeResponseCodeCountDStream.foreachRDD(rdd => {
    val responseCodeToCount: Array[(Int, Long)] = rdd.take(100)
    println( s"""Response code counts: ${responseCodeToCount.mkString("[", ",", "]")}""")
  })

  // A DStream of ipAddresses accessed > 10 times.
  val ipAddressDStream: DStream[String] = accessLogsDStream
    .transform(ipAddressCount)
    .updateStateByKey(computeRunningSum)
    .transform(filterIPAddress)
  ipAddressDStream.foreachRDD(rdd => {
    val ipAddresses: Array[String] = rdd.take(100)
    println( s"""IPAddresses > 10 times: ${ipAddresses.mkString("[", ",", "]")}""")
  })

  // A DStream of endpoint to count.
  val endpointCountsDStream: DStream[(String, Long)] = accessLogsDStream
    .transform(endpointCount)
    .updateStateByKey(computeRunningSum)
  endpointCountsDStream.foreachRDD(rdd => {
    val topEndpoints: Array[(String, Long)] = rdd.top(10)(Ordering.by[(String, Long), Long](_._2))
    println( s"""Top Endpoints: ${topEndpoints.mkString("[", ",", "]")}""")
  })

  streamingContext.start()
  streamingContext.awaitTermination()
}
