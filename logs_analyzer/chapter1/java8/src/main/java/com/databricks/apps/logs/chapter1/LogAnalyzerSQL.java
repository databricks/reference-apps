package com.databricks.apps.logs.chapter1;

import com.databricks.apps.logs.ApacheAccessLog;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.Row;

import scala.Tuple2;

import java.util.List;

/**
 * LogAnalyzerSQL shows how to use SQL syntax with Spark.
 *
 * Example command to run:
 * %  ${YOUR_SPARK_HOME}/bin/spark-submit
 *     --class "com.databricks.apps.logs.chapter1.LogsAnalyzerSQL"
 *     --master local[4]
 *     target/log-analyzer-1.0.jar
 *     ../../data/apache.accesslog
 */
public class LogAnalyzerSQL {

  public static void main(String[] args) {
    // Initialize the Spark context.
    SparkConf conf = new SparkConf().setAppName("Log Analyzer SQL");
    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(sc);

    if (args.length == 0) {
      System.out.println("Must specify an access logs file.");
      System.exit(-1);
    }
    String logFile = args[0];
    JavaRDD<ApacheAccessLog> accessLogs = sc.textFile(logFile)
        .map(ApacheAccessLog::parseFromLogLine);

    // Spark SQL can imply a schema for a table if given a Java class with getters and setters.
    DataFrame sqlDataFrame =
        sqlContext.createDataFrame(accessLogs, ApacheAccessLog.class);
    sqlDataFrame.registerTempTable("logs");
    sqlContext.cacheTable("logs");

    // Calculate statistics based on the content size.
    Row contentSizeStats = sqlContext.sql(
        "SELECT SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize) FROM logs")
            .javaRDD()
            .collect()
            .get(0);
    System.out.println(String.format("Content Size Avg: %s, Min: %s, Max: %s",
        contentSizeStats.getLong(0) / contentSizeStats.getLong(1),
        contentSizeStats.getLong(2),
        contentSizeStats.getLong(3)));

    // Compute Response Code to Count.
    List<Tuple2<Integer, Long>> responseCodeToCount = sqlContext
        .sql("SELECT responseCode, COUNT(*) FROM logs GROUP BY responseCode LIMIT 100")
        .javaRDD()
        .mapToPair(row -> new Tuple2<>(row.getInt(0), row.getLong(1)))
        .collect();
    System.out.println(String.format("Response code counts: %s", responseCodeToCount));

    // Any IPAddress that has accessed the server more than 10 times.
    List<String> ipAddresses = sqlContext
        .sql("SELECT ipAddress, COUNT(*) AS total FROM logs GROUP BY ipAddress HAVING total > 10 LIMIT 100")
        .javaRDD()
        .map(row -> row.getString(0))
        .collect();
    System.out.println(String.format("IPAddresses > 10 times: %s", ipAddresses));

    // Top Endpoints.
    List<Tuple2<String, Long>> topEndpoints = sqlContext
        .sql("SELECT endpoint, COUNT(*) AS total FROM logs GROUP BY endpoint ORDER BY total DESC LIMIT 10")
        .javaRDD()
        .map(row -> new Tuple2<>(row.getString(0), row.getLong(1)))
        .collect();
    System.out.println(String.format("Top Endpoints: %s", topEndpoints));

    sc.stop();
  }
}
