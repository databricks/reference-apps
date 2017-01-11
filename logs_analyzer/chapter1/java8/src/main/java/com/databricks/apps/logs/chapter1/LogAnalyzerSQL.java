package com.databricks.apps.logs.chapter1;

import java.util.List;

import scala.Tuple2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.databricks.apps.logs.ApacheAccessLog;

/**
 * LogAnalyzerSQL shows how to use SQL syntax with Spark.
 *
 * Example command to run:
 * %  ${YOUR_SPARK_HOME}/bin/spark-submit
 *     --class "com.databricks.apps.logs.chapter1.LogAnalyzerSQL"
 *     --master local[*]
 *     target/log-analyzer-2.0.jar
 *     ../../data/apache.access.log
 */
public class LogAnalyzerSQL {

  public static void main(String[] args) {
    // Initialize SparkSession instance.
    SparkSession spark = SparkSession
            .builder()
            .appName("Log Analyzer SQL")
            .getOrCreate();

    if (args.length == 0) {
      System.out.println("Must specify an access logs file.");
      System.exit(-1);
    }
    String logFile = args[0];

    // Read Dataset of lines from the file.
    // Note how we convert Dataset of String lines to Dataset of ApacheAccessLog objects
    // using an Encoder.
    Dataset<ApacheAccessLog> accessLogs = spark
            .read()
            .textFile(logFile)
            .map(ApacheAccessLog::parseFromLogLine,
                    Encoders.bean(ApacheAccessLog.class));

    // Register the Dataset as a temporary view.
    accessLogs.createOrReplaceTempView("logs");

    // Calculate statistics based on the content size.
    Row contentSizeStats = spark
            .sql("SELECT SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize) FROM logs")
            .first();
    System.out.println(String.format("Content Size Avg: %s, Min: %s, Max: %s",
        contentSizeStats.getLong(0) / contentSizeStats.getLong(1),
        contentSizeStats.getLong(2),
        contentSizeStats.getLong(3)));

    // Compute Response Code to Count.
    // Note the use of "LIMIT 100" since the number of responseCodes
    // can potentially be too large to fit in memory.
    List<Tuple2<Integer, Long>> responseCodeToCount = spark
        .sql("SELECT responseCode, COUNT(*) FROM logs GROUP BY responseCode LIMIT 100")
        .map(row -> new Tuple2<>(row.getInt(0), row.getLong(1)),
                Encoders.tuple(Encoders.INT(), Encoders.LONG()))
        .collectAsList();
    System.out.println(String.format("Response code counts: %s", responseCodeToCount));

    // Any IPAddress that has accessed the server more than 10 times.
    List<String> ipAddresses = spark
        .sql("SELECT ipAddress, COUNT(*) AS total FROM logs GROUP BY ipAddress HAVING total > 10 LIMIT 100")
        .map(row -> row.getString(0), Encoders.STRING())
        .collectAsList();
    System.out.println(String.format("IPAddresses > 10 times: %s", ipAddresses));

    // Top Endpoints.
    List<Tuple2<String, Long>> topEndpoints = spark
        .sql("SELECT endpoint, COUNT(*) AS total FROM logs GROUP BY endpoint ORDER BY total DESC LIMIT 10")
        .map(row -> new Tuple2<>(row.getString(0), row.getLong(1)),
                Encoders.tuple(Encoders.STRING(), Encoders.LONG()))
        .collectAsList();
    System.out.println(String.format("Top Endpoints: %s", topEndpoints));

    spark.stop();
  }
}
