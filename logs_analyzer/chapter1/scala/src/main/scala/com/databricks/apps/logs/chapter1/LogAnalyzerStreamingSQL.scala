package com.databricks.apps.logs.chapter1

import com.databricks.apps.logs.ApacheAccessLog
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{StreamingContext, Duration}

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
 *   --master local[4]
 *   target/scala-2.10/spark-logs-analyzer_2.10-1.0.jar
 */
object LogAnalyzerStreamingSQL {
  val WINDOW_LENGTH = new Duration(30 * 1000)
  val SLIDE_INTERVAL = new Duration(10 * 1000)

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Log Analyzer Streaming in Scala")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.createSchemaRDD

    val streamingContext = new StreamingContext(sc, SLIDE_INTERVAL)

    val logLinesDStream = streamingContext.socketTextStream("localhost", 9999)

    val accessLogsDStream = logLinesDStream.map(ApacheAccessLog.parseLogLine).cache()

    val windowDStream = accessLogsDStream.window(WINDOW_LENGTH, SLIDE_INTERVAL)

    windowDStream.foreachRDD(accessLogs => {
      if (accessLogs.count() == 0) {
        println("No access com.databricks.app.logs received in this time interval")
      } else {
        accessLogs.registerAsTable("com/databricks/app/logs")

        // Calculate statistics based on the content size.
        val contentSizeStats = sqlContext
          .sql("SELECT SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize) FROM com.databricks.app.logs")
          .first()
        println("Content Size Avg: %s, Min: %s, Max: %s".format(
          contentSizeStats.getLong(0) / contentSizeStats.getLong(1),
          contentSizeStats(2),
          contentSizeStats(3)))

        // Compute Response Code to Count.
        val responseCodeToCount = sqlContext
          .sql("SELECT responseCode, COUNT(*) FROM com.databricks.app.logs GROUP BY responseCode")
          .map(row => (row.getInt(0), row.getLong(1)))
          .take(1000)
        println(s"""Response code counts: ${responseCodeToCount.mkString("[", ",", "]")}""")

        // Any IPAddress that has accessed the server more than 10 times.
        val ipAddresses =sqlContext
          .sql("SELECT ipAddress, COUNT(*) AS total FROM com.databricks.app.logs GROUP BY ipAddress HAVING total > 10")
          .map(row => row.getString(0))
          .take(100)
        println(s"""IPAddresses > 10 times: ${ipAddresses.mkString("[", ",", "]")}""")

        val topEndpoints = sqlContext
          .sql("SELECT endpoint, COUNT(*) AS total FROM com.databricks.app.logs GROUP BY endpoint ORDER BY total DESC LIMIT 10")
          .map(row => (row.getString(0), row.getLong(1)))
          .collect()
        println(s"""Top Endpoints: ${topEndpoints.mkString("[", ",", "]")}""")
      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
