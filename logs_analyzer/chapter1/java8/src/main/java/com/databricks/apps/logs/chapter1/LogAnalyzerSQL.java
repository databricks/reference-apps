package com.databricks.apps.logs.chapter1;

import com.databricks.apps.logs.ApacheAccessLog;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

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
    SparkSession sparkSession = SparkSession
            .builder()
            .appName("Log Analyzer SQL")
            .getOrCreate();
    JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

    if (args.length == 0) {
      System.out.println("Must specify an access logs file.");
      System.exit(-1);
    }
    String logFile = args[0];
    JavaRDD<ApacheAccessLog> accessLogs = sc.textFile(logFile)
        .map(ApacheAccessLog::parseFromLogLine);

    // Create Spark DataFrame from the RDD.
    Dataset<Row> accessLogsDf =
            sparkSession.createDataFrame(accessLogs, ApacheAccessLog.class);
    accessLogsDf.createOrReplaceTempView("logs");

    // Calculate statistics based on the content size.
    Row contentSizeStats = sparkSession
            .sql("SELECT SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize) FROM logs")
            .first();
    System.out.println(String.format("Content Size Avg: %s, Min: %s, Max: %s",
        contentSizeStats.getLong(0) / contentSizeStats.getLong(1),
        contentSizeStats.getLong(2),
        contentSizeStats.getLong(3)));

    // Compute Response Code to Count.
    List<Tuple2<Integer, Long>> responseCodeToCount = sparkSession
        .sql("SELECT responseCode, COUNT(*) FROM logs GROUP BY responseCode LIMIT 100")
        .map(row -> new Tuple2<>(row.getInt(0), row.getLong(1)),
                Encoders.tuple(Encoders.INT(), Encoders.LONG()))
        .collectAsList();
    System.out.println(String.format("Response code counts: %s", responseCodeToCount));

    // Any IPAddress that has accessed the server more than 10 times.
    List<String> ipAddresses = sparkSession
        .sql("SELECT ipAddress, COUNT(*) AS total FROM logs GROUP BY ipAddress HAVING total > 10 LIMIT 100")
        .map(row -> row.getString(0), Encoders.STRING())
        .collectAsList();
    System.out.println(String.format("IPAddresses > 10 times: %s", ipAddresses));

    // Top Endpoints.
    List<Tuple2<String, Long>> topEndpoints = sparkSession
        .sql("SELECT endpoint, COUNT(*) AS total FROM logs GROUP BY endpoint ORDER BY total DESC LIMIT 10")
        .map(row -> new Tuple2<>(row.getString(0), row.getLong(1)),
                Encoders.tuple(Encoders.STRING(), Encoders.LONG()))
        .collectAsList();
    System.out.println(String.format("Top Endpoints: %s", topEndpoints));

    sc.stop();
  }
}
