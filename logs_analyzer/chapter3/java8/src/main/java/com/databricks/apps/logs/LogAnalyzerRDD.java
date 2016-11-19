package com.databricks.apps.logs;

import java.util.List;

import scala.Tuple2;
import scala.Tuple4;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LogAnalyzerRDD {
  private final SparkSession spark;

  public LogAnalyzerRDD(SparkSession spark) {
    this.spark = spark;
  }

  public LogStatistics processRdd(JavaRDD<ApacheAccessLog> accessLogs) {
    // Create Spark DataFrame from the RDD.
    // Spark SQL can imply a schema for a table if given a Java class with getters and setters.
    Dataset<Row> accessLogsDf = spark.createDataFrame(accessLogs, ApacheAccessLog.class);
    // Register the DataFrame as a temporary view.
    accessLogsDf.createOrReplaceTempView("logs");

    LogStatistics stats = new LogStatistics();

    // Calculate statistics based on the content size.
    Row contentSizeStats = spark
            .sql("SELECT SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize) FROM logs")
            .first();
    stats.setContentSizeStats(
            new Tuple4<>(contentSizeStats.getLong(0),
                    contentSizeStats.getLong(1),
                    contentSizeStats.getLong(2),
                    contentSizeStats.getLong(3)));

    // Compute Response Code to Count.
    List<Tuple2<Integer, Long>> responseCodeToCount = spark
        .sql("SELECT responseCode, COUNT(*) FROM logs GROUP BY responseCode LIMIT 100")
        .map(row -> new Tuple2<>(row.getInt(0), row.getLong(1)),
                Encoders.tuple(Encoders.INT(), Encoders.LONG()))
        .collectAsList();
    stats.setResponseCodeToCount(responseCodeToCount);

    // Any IPAddress that has accessed the server more than 10 times.
    List<String> ipAddresses = spark
        .sql("SELECT ipAddress, COUNT(*) AS total FROM logs GROUP BY ipAddress HAVING total > 10 LIMIT 100")
        .map(row -> row.getString(0), Encoders.STRING())
        .collectAsList();
    stats.setIpAddresses(ipAddresses);

    // Top Endpoints.
    List<Tuple2<String, Long>> topEndpoints = spark
        .sql("SELECT endpoint, COUNT(*) AS total FROM logs GROUP BY endpoint ORDER BY total DESC LIMIT 10")
        .map(row -> new Tuple2<>(row.getString(0), row.getLong(1)),
                Encoders.tuple(Encoders.STRING(), Encoders.LONG()))
        .collectAsList();
    stats.setTopEndpoints(topEndpoints);

    return stats;
  }
}
