package com.databricks.apps.logs.chapter1

import java.util.concurrent.atomic.AtomicLong

import com.databricks.apps.logs.{ApacheAccessLog, OrderingUtils}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}

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
 *   target/scala-2.10/spark-logs-analyzer_2.10-1.0.jar
 */
object LogAnalyzerStreamingTotalRefactored {
  val WINDOW_LENGTH = new Duration(30 * 1000)
  val SLIDE_INTERVAL = new Duration(10 * 1000)

  val computeRunningSum = (values: Seq[Long], state: Option[Long]) => {
    val currentCount = values.foldLeft(0L)(_ + _)
    val previousCount = state.getOrElse(0L)
    Some(currentCount + previousCount)
  }

  val runningCount = new AtomicLong(0)
  val runningSum = new AtomicLong(0)
  val runningMin = new AtomicLong(Long.MaxValue)
  val runningMax = new AtomicLong(Long.MinValue)

  val contentSizeStats = (accessLogRDD: RDD[ApacheAccessLog]) => {
    val contentSizes = accessLogRDD.map(log => log.contentSize).cache()
    (contentSizes.count(), contentSizes.reduce(_ + _),
      contentSizes.min, contentSizes.max)
  }

  val responseCodeCount = (accessLogRDD: RDD[ApacheAccessLog]) => {
    accessLogRDD.map(log => (log.responseCode, 1L)).reduceByKey(_ + _)
  }

  val ipAddressCount = (accessLogRDD: RDD[ApacheAccessLog]) =>  {
    accessLogRDD.map(log => (log.ipAddress, 1L)).reduceByKey(_ + _)
  }

  val filterIPAddress = (ipAddressCount: RDD[(String, Long)]) => {
    ipAddressCount.filter(_._2 > 10).map(_._1)
  }

  val endpointCount = (accessLogRDD: RDD[ApacheAccessLog]) => {
    accessLogRDD.map(log => (log.endpoint, 1L)).reduceByKey(_ + _)
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Log Analyzer Streaming Total in Scala")
    val sc = new SparkContext(sparkConf)

    val streamingContext = new StreamingContext(sc, SLIDE_INTERVAL)

    // NOTE: Checkpointing must be enabled to use updateStateByKey.
    streamingContext.checkpoint("/tmp/log-analyzer-streaming-total-scala")

    val logLinesDStream = streamingContext.socketTextStream("localhost", 9999)

    val accessLogsDStream = logLinesDStream.map(ApacheAccessLog.parseLogLine).cache()

    // Calculate statistics based on the content size.
    accessLogsDStream.foreachRDD(rdd => {
      val count = rdd.count()
      if (count > 0) {
        val currentContentSizes = contentSizeStats(rdd)
        runningCount.getAndAdd(currentContentSizes._1)
        runningSum.getAndAdd(currentContentSizes._2)
        runningMin.set(currentContentSizes._3)
        runningMax.set(currentContentSizes._4)
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
    val responseCodeCountDStream = accessLogsDStream
      .transform(responseCodeCount)
    val cumulativeResponseCodeCountDStream = responseCodeCountDStream
      .updateStateByKey(computeRunningSum)

    cumulativeResponseCodeCountDStream .foreachRDD(rdd => {
      val responseCodeToCount = rdd.take(100)
      println( s"""Response code counts: ${responseCodeToCount.mkString("[", ",", "]")}""")
    })

    val ipAddressDStream = accessLogsDStream
      .transform(ipAddressCount)
      .updateStateByKey(computeRunningSum)
      .transform(filterIPAddress)
    ipAddressDStream.foreachRDD(rdd => {
      val ipAddresses = rdd.take(100)
      println( s"""IPAddresses > 10 times: ${ipAddresses.mkString("[", ",", "]")}""")
    })

    val endpointCountsDStream = accessLogsDStream
      .transform(endpointCount)
      .updateStateByKey(computeRunningSum)
    endpointCountsDStream.foreachRDD(rdd => {
      val topEndpoints = rdd.top(10)(OrderingUtils.SecondValueLongOrdering)
      println( s"""Top Endpoints: ${topEndpoints.mkString("[", ",", "]")}""")
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
