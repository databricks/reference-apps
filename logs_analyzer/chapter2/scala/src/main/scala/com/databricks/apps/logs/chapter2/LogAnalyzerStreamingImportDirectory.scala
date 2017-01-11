package com.databricks.apps.logs.chapter2

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import com.databricks.apps.logs.{ApacheAccessLog, LogAnalyzerRDD}

/**
  * The LogAnalyzerImportStreamingFile illustrates how to run Spark Streaming,
  * but instead of monitoring a socket, it monitors a directory and feeds
  * in any new files to streaming.
  *
  * Once you get this program up and running, feed apache access log files
  * into that directory.
  *
  * Example command to run:
  * %  ${YOUR_SPARK_HOME}/bin/spark-submit
  * --class "com.databricks.apps.logs.chapter2.LogAnalyzerStreamingImportDirectory"
  * --master spark://YOUR_SPARK_MASTER
  * target/scala-2.11/spark-logs-analyzer_2.11-2.0.jar
  * YOUR_LOCAL_LOGS_DIRECTORY
  */
object LogAnalyzerStreamingImportDirectory extends App {
  val WINDOW_LENGTH = Seconds(30)
  val SLIDE_INTERVAL = Seconds(10)

  val spark = SparkSession
    .builder()
    .appName("Log Analyzer Import Streaming HDFS")
    .getOrCreate()
  val streamingContext = new StreamingContext(spark.sparkContext, SLIDE_INTERVAL)

  val directory = args(0)

  // This method monitors a directory for new files to read in for streaming.
  val logData: DStream[String] = streamingContext.textFileStream(directory)

  val accessLogsDStream: DStream[ApacheAccessLog] = logData.map(ApacheAccessLog.parseLogLine)
  val windowDStream: DStream[ApacheAccessLog] = accessLogsDStream.window(WINDOW_LENGTH, SLIDE_INTERVAL)

  val logAnalyzerRDD = LogAnalyzerRDD(spark)
  windowDStream.foreachRDD(accessLogs => {
    if (accessLogs.count() == 0) {
      println("No access logs received in this time interval")
    } else {
      val logStatistics = logAnalyzerRDD.processRdd(accessLogs)
      logStatistics.printToStandardOut()
    }
  })

  // Start the streaming server.
  streamingContext.start() // Start the computation
  streamingContext.awaitTermination() // Wait for the computation to terminate
}
