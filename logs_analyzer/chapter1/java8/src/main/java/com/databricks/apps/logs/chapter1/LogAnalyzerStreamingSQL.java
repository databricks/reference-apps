package com.databricks.apps.logs.chapter1;

import java.util.List;

import scala.Tuple2;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.databricks.apps.logs.ApacheAccessLog;

/**
 * The LogAnalyzerStreamingSQL is similar to LogAnalyzerStreaming, except
 * it computes stats using Spark SQL.
 *
 * To feed the new lines of some logfile into a socket for streaming,
 * Run this command:
 *   % tail -f [[YOUR_LOG_FILE]] | nc -lk 9999
 *
 * If you don't have a live log file that is being written to,
 * you can add test lines using this command:
 *   % cat ../../data/apache.access.log >> [[YOUR_LOG_FILE]]
 *
 * Example command to run:
 * %  ${YOUR_SPARK_HOME}/bin/spark-submit
 *     --class "com.databricks.apps.logs.chapter1.LogAnalyzerStreamingSQL"
 *     --master local[*]
 *     target/log-analyzer-2.0.jar
 *
 * On another console, run the shell script that emulates network stream
 * by periodically sending portions of the sample log file to a network socket:
 * % cd ../../data
 * % ./stream.sh apache.access.log
 */
public class LogAnalyzerStreamingSQL {
  // Stats will be computed for the last window length of time.
  private static final Duration WINDOW_LENGTH = new Duration(30 * 1000);
  // Stats will be computed every slide interval time.
  private static final Duration SLIDE_INTERVAL = new Duration(10 * 1000);

  public static void main(String[] args) throws InterruptedException {
    // Initialize SparkSession instance.
    // Note: Only SparkSession instance is created,
    //other flavors of Spark context are obtained from it.
    SparkSession spark = SparkSession
            .builder()
            .appName("Log Analyzer Streaming SQL")
            .getOrCreate();
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    JavaStreamingContext jssc = new JavaStreamingContext(sc,
        SLIDE_INTERVAL);  // This sets the update window to be every 10 seconds.

    JavaReceiverInputDStream<String> logDataDStream =
        jssc.socketTextStream("localhost", 9999);


    // A DStream of RDD's that contain parsed Apache Access Logs.
    JavaDStream<ApacheAccessLog> accessLogDStream =
        logDataDStream.map(ApacheAccessLog::parseFromLogLine);

    // Splits the accessLogDStream into a dstream of time windowed rdd's.
    JavaDStream<ApacheAccessLog> windowDStream =
        accessLogDStream.window(WINDOW_LENGTH, SLIDE_INTERVAL);

    windowDStream.foreachRDD(accessLogs -> {
      if (accessLogs.count() == 0) {
        System.out.println("No access logs in this time interval");
        return;
      }

      // *** Note that this is code copied verbatim from LogAnalyzerSQL.java.
      // Create Spark DataFrame from the RDD.
      Dataset<Row> accessLogsDf =
              spark.createDataFrame(accessLogs, ApacheAccessLog.class);
      accessLogsDf.createOrReplaceTempView("logs");

      // Calculate statistics based on the content size.
      Row contentSizeStats = spark
          .sql("SELECT SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize) FROM logs")
          .first();
      System.out.println(String.format("Content Size Avg: %s, Min: %s, Max: %s",
          contentSizeStats.getLong(0) / contentSizeStats.getLong(1),
          contentSizeStats.getLong(2),
          contentSizeStats.getLong(3)));

      // Compute Response Code to Count.
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
    });

    // Start the streaming server.
    jssc.start();              // Start the computation
    jssc.awaitTermination();   // Wait for the computation to terminate
  }
}
