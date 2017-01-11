package com.databricks.apps.logs.chapter3;

import java.io.*;
import java.util.List;

import scala.Tuple2;
import scala.Tuple4;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import com.databricks.apps.logs.ApacheAccessLog;
import com.databricks.apps.logs.LogAnalyzerRDD;
import com.databricks.apps.logs.LogStatistics;

/**
 * LogAnalyzerExportSmallData shows how to export data of small size to a file.
 * 
  * Example command to run:
 * %  ${YOUR_SPARK_HOME}/bin/spark-submit
 *     --class "com.databricks.apps.logs.chapter3.LogAnalyzerExportSmallData"
 *     --master spark://YOUR_SPARK_MASTER
 *     target/log-analyzer-2.0.jar
 *     ../../data/apache.access.log output.log
 */
public class LogAnalyzerExportSmallData {

  public static void main(String[] args) throws IOException {
      // Initialize SparkSession instance.
    SparkSession spark = SparkSession
            .builder()
            .appName("Log Analyzer SQL")
            .getOrCreate();

    if (args.length < 2) {
      System.out.println("Must specify an access logs file and an output file.");
      System.exit(-1);
    }
    String logFile = args[0];
    JavaRDD<ApacheAccessLog> accessLogs = spark
        .read()
        .textFile(logFile)
        .javaRDD()
        .map(ApacheAccessLog::parseFromLogLine);

    LogAnalyzerRDD logAnalyzerRDD = new LogAnalyzerRDD(spark);
    LogStatistics logStatistics = logAnalyzerRDD.processRdd(accessLogs);

    String outputFile = args[1];
    try (Writer out = new BufferedWriter(
            new OutputStreamWriter(new FileOutputStream(outputFile)))) {
        Tuple4<Long, Long, Long, Long> contentSizeStats =
                logStatistics.getContentSizeStats();
        out.write(String.format("Content Size Avg: %s, Min: %s, Max: %s\n",
                contentSizeStats._1() / contentSizeStats._2(),
                contentSizeStats._3(),
                contentSizeStats._4()));

        List<Tuple2<Integer, Long>> responseCodeToCount =
                logStatistics.getResponseCodeToCount();
        out.write(String.format("Response code counts: %s\n", responseCodeToCount));

        List<String> ipAddresses = logStatistics.getIpAddresses();
        out.write(String.format("IPAddresses > 10 times: %s\n", ipAddresses));

        List<Tuple2<String, Long>> topEndpoints = logStatistics.getTopEndpoints();
        out.write(String.format("Top Endpoints: %s\n", topEndpoints));
    }

    spark.stop();
  }
}
