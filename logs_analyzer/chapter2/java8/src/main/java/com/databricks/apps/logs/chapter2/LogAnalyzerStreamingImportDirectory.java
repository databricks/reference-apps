package com.databricks.apps.logs.chapter2;

import com.databricks.apps.logs.ApacheAccessLog;
import com.databricks.apps.logs.LogAnalyzerRDD;
import com.databricks.apps.logs.LogStatistics;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

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
 *     YOUR_LOCAL_LOGS_DIRECTORY
 *     target/log-analyzer-1.0.jar
 */
public class LogAnalyzerStreamingImportDirectory {
  private static final Duration WINDOW_LENGTH = new Duration(30 * 1000);
  private static final Duration SLIDE_INTERVAL = new Duration(10 * 1000);

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Log Analyzer Import Streaming HDFS");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaStreamingContext jssc = new JavaStreamingContext(sc, SLIDE_INTERVAL);
    JavaSQLContext sqlContext = new JavaSQLContext(sc);

    // Specify a directory to monitor for log files.
    if (args.length == 0) {
      System.out.println("Must specify an access logs directory.");
      System.exit(-1);
    }
    String directory = args[0];

    // This methods monitors a directory for new files to read in for streaming.
    JavaDStream<String> logData = jssc.textFileStream(directory);

    JavaDStream<ApacheAccessLog> accessLogsDStream
        = logData.map(ApacheAccessLog::parseFromLogLine).cache();

    JavaDStream<ApacheAccessLog> windowDStream = accessLogsDStream.window(
        WINDOW_LENGTH, SLIDE_INTERVAL);

    final LogAnalyzerRDD logAnalyzerRDD = new LogAnalyzerRDD(sqlContext);
    windowDStream.foreachRDD(accessLogs -> {
      if (accessLogs.count() == 0) {
        System.out.println("No access logs in this time interval");
        return null;
      }

      LogStatistics logStatistics = logAnalyzerRDD.processRdd(accessLogs);
      logStatistics.printToStandardOut();

      return null;
    });

    // Start the streaming server.
    jssc.start();              // Start the computation
    jssc.awaitTermination();   // Wait for the computation to terminate
  }
}
