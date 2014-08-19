package com.databricks.apps.logs.chapter2;

import com.databricks.apps.logs.ApacheAccessLog;
import com.databricks.apps.logs.LogAnalyzerRDD;
import com.databricks.apps.logs.LogStatistics;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSQLContext;
import scala.Tuple2;
import scala.Tuple4;

import java.io.*;
import java.util.List;

public class LogAnalyzerExportSmallData {
  public static void main(String[] args) throws IOException {
    // Create the spark context.
    SparkConf conf = new SparkConf().setAppName("Log Analyzer SQL");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaSQLContext sqlContext = new JavaSQLContext(sc);

    if (args.length < 2) {
      System.out.println("Must specify an access logs file and an output file.");
      System.exit(-1);
    }
    String logFile = args[0];
    JavaRDD<ApacheAccessLog> accessLogs = sc.textFile(logFile)
        .map(ApacheAccessLog::parseFromLogLine);

    LogAnalyzerRDD logAnalyzerRDD = new LogAnalyzerRDD(sqlContext);
    LogStatistics logStatistics = logAnalyzerRDD.processRdd(accessLogs);

    String outputFile = args[1];
    Writer out = new BufferedWriter(
        new OutputStreamWriter(new FileOutputStream(outputFile)));

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

    out.close();

    sc.stop();
  }
}
