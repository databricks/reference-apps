package com.databricks.apps.logs.chapter3;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.databricks.apps.logs.ApacheAccessLog;

/**
 * LogAnalyzerExportRDD shows how to export data a set of files.
 *
 * Example command to run:
 * %  ${YOUR_SPARK_HOME}/bin/spark-submit
 *     --class "com.databricks.apps.logs.chapter3.LogAnalyzerExportRDD"
 *     --master spark://YOUR_SPARK_MASTER
 *     target/log-analyzer-2.0.jar
 *     ../../data/apache.access.log output_dir
 */
public class LogAnalyzerExportRDD {
  // Optionally modify this based as makes sense for your dataset.
  public static final int NUM_PARTITIONS = 2;

  public static void main(String[] args) throws IOException {
    // Create the spark context.
    SparkConf conf = new SparkConf().setAppName("Log Analyzer SQL");
    JavaSparkContext sc = new JavaSparkContext(conf);

    if (args.length < 2) {
      System.out.println("Must specify an access logs file and an output directory.");
      System.exit(-1);
    }
    String inputFile = args[0];
    String outputDirectory = args[1];
    JavaRDD<ApacheAccessLog> accessLogs = sc.textFile(inputFile)
        .map(ApacheAccessLog::parseFromLogLine)
        .repartition(NUM_PARTITIONS); // Optionally, change this.

    accessLogs.saveAsTextFile(outputDirectory);

    sc.stop();
  }
}
