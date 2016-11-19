package com.databricks.apps.logs.chapter2;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.databricks.apps.logs.ApacheAccessLog;
import com.databricks.apps.logs.LogAnalyzerRDD;
import com.databricks.apps.logs.LogStatistics;

/**
 * The LogAnalyzerImportStreamingFile illustrates how to run Spark Streaming,
 *   but instead of monitoring a socket, it monitors a directory and feeds
 *   in any new files to streaming.
 *
 * Once you get this program up and running, feed apache access log files
 * into that directory.
 *
 * Example command to run:
 * %  ${YOUR_SPARK_HOME}/bin/spark-submit
 *     --class "com.databricks.apps.logs.chapter2.LogAnalyzerStreamingImportDirectory"
 *     --master spark://YOUR_SPARK_MASTER
 *     target/log-analyzer-2.0.jar
 *     YOUR_LOCAL_LOGS_DIRECTORY
 */
public class LogAnalyzerStreamingImportDirectory {
  private static final Duration WINDOW_LENGTH = new Duration(30 * 1000);
  private static final Duration SLIDE_INTERVAL = new Duration(10 * 1000);

  public static void main(String[] args) throws InterruptedException {
    SparkSession spark = SparkSession
            .builder()
            .appName("Log Analyzer Import Streaming HDFS")
            .getOrCreate();
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    JavaStreamingContext jssc = new JavaStreamingContext(sc, SLIDE_INTERVAL);

    // Specify a directory to monitor for log files.
    if (args.length == 0) {
      System.out.println("Must specify an access logs directory.");
      System.exit(-1);
    }
    String directory = args[0];

    // This method monitors a directory for new files to read in for streaming.
    JavaDStream<String> logData = jssc.textFileStream(directory);

    JavaDStream<ApacheAccessLog> accessLogsDStream
        = logData.map(ApacheAccessLog::parseFromLogLine).cache();

    JavaDStream<ApacheAccessLog> windowDStream = accessLogsDStream.window(
        WINDOW_LENGTH, SLIDE_INTERVAL);

    final LogAnalyzerRDD logAnalyzerRDD = new LogAnalyzerRDD(spark);
    windowDStream.foreachRDD(accessLogs -> {
      if (accessLogs.count() == 0) {
        System.out.println("No access logs in this time interval");
        return;
      }

      LogStatistics logStatistics = logAnalyzerRDD.processRdd(accessLogs);
      logStatistics.printToStandardOut();
    });

    // Start the streaming server.
    jssc.start();              // Start the computation
    jssc.awaitTermination();   // Wait for the computation to terminate
  }
}
