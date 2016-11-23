package com.databricks.apps.logs;

import java.io.IOException;
import java.util.Collections;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

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
 * Modify the command line flags to the values of your choosing.
 * Notice how they come after you specify the jar when using spark-submit.
 *
 * Example command to run:
 * %  ${YOUR_SPARK_HOME}/bin/spark-submit
 *     --class "com.databricks.apps.logs.LogAnalyzerAppMain"
 *     --master spark://YOUR_SPARK_MASTER
 *     target/uber-log-analyzer-2.0.jar
 *     --logs-directory /tmp/logs
 *     --output-html-file /tmp/log_stats.html
 *     --window-length 30
 *     --slide-interval 5
 *     --checkpoint-directory /tmp/log-analyzer-streaming
 */
public class LogAnalyzerAppMain {
  static final String WINDOW_LENGTH = "w";
  static final String SLIDE_INTERVAL = "s";
  static final String LOGS_DIRECTORY = "l";
  static final String OUTPUT_HTML_FILE = "o";
  static final String CHECKPOINT_DIRECTORY = "c";
  static final String HELP = "h";

  private static final Options THE_OPTIONS = createOptions();

  private static Options createOptions() {
    Options options = new Options();
    options.addOption(LOGS_DIRECTORY, "logs-directory", true, "Directory with input log files");
    options.addOption(OUTPUT_HTML_FILE, "output-html-file", true, "Output HTML file to write statistics");
    options.addOption(WINDOW_LENGTH, "window-length", true, "Length of the aggregate window in seconds");
    options.addOption(SLIDE_INTERVAL, "slide-interval", true, "Slide interval in seconds");
    options.addOption(CHECKPOINT_DIRECTORY, "checkpoint-directory", true, "Directory for Spark checkpoints");
    options.addOption(HELP, "help", false, "Print help");
    return options;
  }

  private static void helpAndExit(int status) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("Program [options]", THE_OPTIONS);
    System.exit(status);
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length == 0) {
      helpAndExit(1);
    }
    Flags.setFromCommandLineArgs(THE_OPTIONS, args);
    if (Flags.getInstance().isHelp()) {
      helpAndExit(0);
    }

    // Startup the Spark Conf.
    SparkConf conf = new SparkConf()
        .setAppName("A Databricks Reference Application: Logs Analysis with Spark");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaStreamingContext jssc = new JavaStreamingContext(sc,
        Flags.getInstance().getSlideInterval());

    // Checkpointing must be enabled to use the updateStateByKey function.
    jssc.checkpoint(Flags.getInstance().getCheckpointDirectory());

    // This methods monitors a directory for new files to read in for streaming.
    JavaDStream<String> logData = jssc.textFileStream(Flags.getInstance().getLogsDirectory());
    
    JavaDStream<ApacheAccessLog> accessLogsDStream = logData.flatMap(
        line -> {
            try {
                return Collections.singleton(ApacheAccessLog.parseFromLogLine(line)).iterator();
            } catch (IOException e) {
                return Collections.emptyIterator();
            }
        }
    ).cache();

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
    });

    // Start the streaming server.
    jssc.start();              // Start the computation
    jssc.awaitTermination();   // Wait for the computation to terminate
  }
}
