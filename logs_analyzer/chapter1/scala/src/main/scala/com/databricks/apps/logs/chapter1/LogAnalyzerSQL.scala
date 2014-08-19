package com.databricks.apps.logs.chapter1

import com.databricks.apps.logs.ApacheAccessLog
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * LogAnalyzerSQL shows how to use SQL syntax with Spark.
 *
 * Example command to run:
 * % spark-submit
 *   --class "com.databricks.apps.logs.chapter1.LogAnalyzerSQL"
 *   --master local[4]
 *   target/scala-2.10/spark-logs-analyzer_2.10-1.0.jar
 *   ../../data/apache.access.log
 */
object LogAnalyzerSQL {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Log Analyzer SQL in Scala")
    val sc = new SparkContext(sparkConf)

    val logFile = args(0)

    val sqlContext = new SQLContext(sc)
    import sqlContext.createSchemaRDD

    val accessLogs = sc.textFile(logFile).map(ApacheAccessLog.parseLogLine).cache()
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

    sc.stop()
  }
}
