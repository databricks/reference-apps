package com.databricks.apps.logs.chapter2;

import com.databricks.apps.logs.ApacheAccessLog;
import com.databricks.apps.logs.LogAnalyzerRDD;
import com.databricks.apps.logs.LogStatistics;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSQLContext;

public class LogAnalyzerBatchImport {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Log Analyzer SQL");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaSQLContext sqlContext = new JavaSQLContext(sc);

    if (args.length == 0) {
      System.out.println("Must specify an access logs file.");
      System.exit(-1);
    }
    String logFile = args[0];
    JavaRDD<ApacheAccessLog> accessLogs = sc.textFile(logFile)
        .map(ApacheAccessLog::parseFromLogLine);

    LogAnalyzerRDD logAnalyzerRDD = new LogAnalyzerRDD(sqlContext);
    LogStatistics logStatistics = logAnalyzerRDD.processRdd(accessLogs);
    logStatistics.printToStandardOut();

    sc.stop();
  }
}
