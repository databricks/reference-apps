package com.databricks.apps.logs

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Analyzer for RDD of ApacheAccessLog entries.
  */
case class LogAnalyzerRDD(spark: SparkSession) {
  def processRdd(accessLogs: RDD[ApacheAccessLog]): LogStatistics = {
    // For implicit conversions from RDDs to DataFrames
    import spark.implicits._
    // Create Spark DataFrame from the RDD.
    val accessLogsDf = accessLogs.toDF()

    // Register the DataFrame as a temporary view.
    accessLogsDf.createOrReplaceTempView("logs")


    // Calculate statistics based on the content size.
    val contentSizeStats = spark
      .sql("SELECT SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize) FROM logs")
      .first()

    // Compute Response Code to Count.
    val responseCodeToCount = spark
      .sql("SELECT responseCode, COUNT(*) FROM logs GROUP BY responseCode LIMIT 1000")
      .map(row => (row.getInt(0), row.getLong(1)))
      .collect()

    // Any IPAddress that has accessed the server more than 10 times.
    val ipAddresses = spark
      .sql("SELECT ipAddress, COUNT(*) AS total FROM logs GROUP BY ipAddress HAVING total > 10 LIMIT 1000")
      .map(_.getString(0))
      .collect()

    // Top Endpoints.
    val topEndpoints = spark
      .sql("SELECT endpoint, COUNT(*) AS total FROM logs GROUP BY endpoint ORDER BY total DESC LIMIT 10")
      .map(row => (row.getString(0), row.getLong(1)))
      .collect()

    LogStatistics(
      (contentSizeStats.getLong(0),
        contentSizeStats.getLong(1),
        contentSizeStats.getLong(2),
        contentSizeStats.getLong(3)),
      responseCodeToCount,
      ipAddresses,
      topEndpoints)
  }
}