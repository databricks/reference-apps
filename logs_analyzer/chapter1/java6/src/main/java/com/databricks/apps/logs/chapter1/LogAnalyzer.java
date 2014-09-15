package com.databricks.apps.logs.chapter1;

import com.databricks.apps.logs.ApacheAccessLog;
import com.databricks.apps.logs.Functions;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;



/**
 * The LogAnalyzer takes in an apache access log file and
 * computes some statistics on them.
 *
 * Example command to run:
 * %  ${YOUR_SPARK_HOME}/bin/spark-submit
 *     --class "com.databricks.apps.logs.chapter1.LogsAnalyzer"
 *     --master local[4]
 *     target/log-analyzer-1.0.jar
 *     ../../data/apache.accesslog
 */
public class LogAnalyzer {
  public static void main(String[] args) {
    // Create a Spark Context.
    SparkConf conf = new SparkConf().setAppName("Log Analyzer");
    JavaSparkContext sc = new JavaSparkContext(conf);

    // Load the text file into Spark.
    if (args.length == 0) {
      System.out.println("Must specify an access logs file.");
      System.exit(-1);
    }
    String logFile = args[0];
    JavaRDD<String> logLines = sc.textFile(logFile);

    // Convert the text log lines to ApacheAccessLog objects and cache them
    //   since multiple transformations and actions will be called on that data.
    JavaRDD<ApacheAccessLog> accessLogs =
       logLines.map(Functions.PARSE_LOG_LINE).cache();

    // Calculate statistics based on the content size.
    // Note how the contentSizes are cached as well since multiple actions
    //   are called on that RDD.
    JavaRDD<Long> contentSizes =
        accessLogs.map(Functions.GET_CONTENT_SIZE).cache();
    System.out.print("Content Size Avg: " +
        contentSizes.reduce(Functions.SUM_REDUCER) / contentSizes.count());
    System.out.print(", Min: " +
        contentSizes.min(Functions.LONG_NATURAL_ORDER_COMPARATOR));
    System.out.println(", Max: " +
        contentSizes.max(Functions.LONG_NATURAL_ORDER_COMPARATOR));

    // Compute Response Code to Count.
    List<Tuple2<Integer, Long>> responseCodeToCount =
        accessLogs.mapToPair(Functions.GET_RESPONSE_CODE)
            .reduceByKey(Functions.SUM_REDUCER)
            .take(100);
    System.out.println("Response code counts: " + responseCodeToCount);

    // Any IPAddress that has accessed the server more than 10 times.

    List<String> ipAddresses = accessLogs
        .mapToPair(Functions.GET_IP_ADDRESS)
        .reduceByKey(Functions.SUM_REDUCER)
        .filter(Functions.FILTER_GREATER_10)
        .map(Functions.GET_TUPLE_FIRST)
        .take(100);
    System.out.println("IPAddresses > 10 times: " + ipAddresses);

    // Top Endpoints.
    List<Tuple2<String, Long>> topEndpoints = accessLogs
        .mapToPair(Functions.GET_ENDPOINT)
        .reduceByKey(Functions.SUM_REDUCER)
        .top(10, new Functions.ValueComparator<String, Long>(
            Functions.LONG_NATURAL_ORDER_COMPARATOR));
    System.out.println("Top Endpoints: " + topEndpoints);

    sc.stop();
  }
}

