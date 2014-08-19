package com.databricks.apps.logs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.IOException;

/**
 * The LogAnalyzerAppMain is an sample logs analysis application.  For now,
 * it is a simple minimal viable product:
 *   - Read in new log files from a directory and input those new files into streaming.
 *   - Computes stats for all of time as well as the last time interval based on those logs.
 *   - Write the calculated stats to an txt file on the local file system
 *     that gets refreshed every time interval.
 *
 * Once you get this program up and running, feed apache access log files
 * into the local directory of your choosing.
 *
 * Then open your output text file, perhaps in a web browser, and refresh
 * that page to see more stats come in.
 *
 * Modify the hardcoded FLAGS variables to the values of your choosing.
 *
 * Example command to run:
 * %  ${YOUR_SPARK_HOME}/bin/spark-submit
 *     --class "com.databricks.apps.logs.LogAnalyzerAppMain"
 *     --master local[4]
 *     target/log-analyzer-1.0.jar
 */
public class LogAnalyzerAppMain {
  public static class Flags {
    private static final Flags THE_INSTANCE = new Flags();

    private final Duration windowLength = new Duration(30 * 1000);
    private final Duration slideInterval = new Duration(5 * 1000);

    private final String logsDirectory = "/tmp/logs";
    private final String outputHtmlFile = "/tmp/log_stats.html";

    private final String checkpointDirectory =
        "/tmp/log-analyzer-streaming";

    // The path to this project's index.html.template
    // This file needs to be accessible to all Spark workers.
    private final String indexHtmlTemplate =
        "./src/main/resources/index.html.template";

    private Flags() {}

    public static Flags getInstance() {
      return THE_INSTANCE;
    }

    public Duration getWindowLength() {
      return windowLength;
    }

    public Duration getSlideInterval() {
      return slideInterval;
    }

    public String getLogsDirectory() {
      return logsDirectory;
    }

    public String getOutputHtmlFile() {
      return outputHtmlFile;
    }

    public String getCheckpointDirectory() {
      return checkpointDirectory;
    }

    public String getIndexHtmlTemplate() {
      return indexHtmlTemplate;
    }
  }

  public static void main(String[] args) throws IOException {
    SparkConf conf = new SparkConf()
        .setAppName("A Databricks Reference Application: Logs Analysis with Spark");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaStreamingContext jssc = new JavaStreamingContext(sc,
        Flags.getInstance().getSlideInterval());

    // Checkpointing must be enabled to use the updateStateByKey function.
    jssc.checkpoint(Flags.getInstance().getCheckpointDirectory());

    // This methods monitors a directory for new files to read in for streaming.
    JavaDStream<String> logData = jssc.textFileStream(Flags.getInstance().getLogsDirectory());

    JavaDStream<ApacheAccessLog> accessLogsDStream
        = logData.map(ApacheAccessLog::parseFromLogLine).cache();

    LogAnalyzerTotal logAnalyzerTotal = new LogAnalyzerTotal();
    LogAnalyzerWindowed logAnalyzerWindowed = new LogAnalyzerWindowed();

    // Process the DStream which gathers stats for all of time.
    logAnalyzerTotal.processAccessLogs(accessLogsDStream);

    // Calculate statistics for the last time interval.
    logAnalyzerWindowed.processAccessLogs(accessLogsDStream);

    // Render the output each time there is a new RDD in the accessLogsDStream.
    Renderer renderer = new Renderer();
    accessLogsDStream.foreachRDD(rdd -> {
      // Call this to output the stats.
      renderer.render(logAnalyzerTotal.getLogStatistics(),
          logAnalyzerWindowed.getLogStatistics());
      return null;
    });

    // Start the streaming server.
    jssc.start();              // Start the computation
    jssc.awaitTermination();   // Wait for the computation to terminate
  }
}
