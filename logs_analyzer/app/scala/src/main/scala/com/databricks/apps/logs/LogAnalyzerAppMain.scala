package com.databricks.apps.logs

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * The LogAnalyzerAppMain is an sample logs analysis application.  For now,
  * it is a simple minimal viable product:
  *   - Read in new log files from a directory and input those new files into streaming.
  *   - Computes stats for all of time as well as the last time interval based on those logs.
  *   - Write the calculated stats to an txt file on the local file system
  * that gets refreshed every time interval.
  *
  * Once you get this program up and running, feed apache access log files
  * into the local directory of your choosing.
  *
  * Then open your output text file, perhaps in a web browser, and refresh
  * that page to see more stats come in.
  *
  * Modify the command line flags to the values of your choosing.
  * Notice how they come after you specify the jar when using spark-submit.
  *
  * Example command to run:
  * %  ${YOUR_SPARK_HOME}/bin/spark-submit
  * --class "com.databricks.apps.logs.LogAnalyzerAppMain"
  * --master spark://YOUR_SPARK_MASTER
  * target/scala-2.11/spark-logs-analyzer_2.11-2.0-assembly.jar
  * --logs-directory /tmp/logs
  * --output-html-file /tmp/log_stats.html
  * --window-length 30
  * --slide-interval 5
  * --checkpoint-directory /tmp/log-analyzer-streaming
  */
object LogAnalyzerAppMain extends App {
  val appOptions = CmdlineArgumentsParser.parse(args)

  // Create Spark Conf and Spark Streaming Context.
  val sparkConf = new SparkConf().setAppName("A Databricks Reference Application: Logs Analysis with Spark")
  val streamingContext = new StreamingContext(sparkConf, Seconds(appOptions.slideInterval))

  // Checkpointing must be enabled to use the updateStateByKey function.
  streamingContext.checkpoint(appOptions.checkpointDirectory)

  // This methods monitors a directory for new files to read in for streaming.
  val logData: DStream[String] = streamingContext.textFileStream(appOptions.logsDirectory)

  // Create DStream of Apache log entries
  val accessLogsDStream: DStream[ApacheAccessLog] = logData
    .flatMap(line => ApacheAccessLog.parseLogLine(line).iterator)
    .cache()

  // Process the DStream which gathers stats for all of time.
  val logAnalyzerTotal = new LogAnalyzerTotal()
  logAnalyzerTotal.processAccessLogs(accessLogsDStream)

  // Calculate statistics for the last time interval.
  val logAnalyzerWindowed = new LogAnalyzerWindowed(appOptions.windowLength, appOptions.slideInterval)
  logAnalyzerWindowed.processAccessLogs(accessLogsDStream)

  // Render the output each time there is a new RDD in the accessLogsDStream.
  val renderer = new Renderer(appOptions.outputHtmlFile, appOptions.windowLength)
  accessLogsDStream.foreachRDD(rdd =>
    renderer.render(logAnalyzerTotal.getLogStatistics, logAnalyzerWindowed.getLogStatistics))

  // Start the streaming server.
  streamingContext.start() // Start the computation
  streamingContext.awaitTermination() // Wait for the computation to terminate
}
