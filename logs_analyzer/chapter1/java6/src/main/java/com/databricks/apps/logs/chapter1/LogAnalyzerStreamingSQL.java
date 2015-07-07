package com.databricks.apps.logs.chapter1;

import com.databricks.apps.logs.ApacheAccessLog;
import com.databricks.apps.logs.Functions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;
import scala.Tuple4;

import java.util.List;

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
 *   % cat ../../data/apache.accesslog >> [[YOUR_LOG_FILE]]
 *
 * Example command to run:
 * %  ${YOUR_SPARK_HOME}/bin/spark-submit
 *     --class "com.databricks.apps.logs.chapter1.LogAnalyzerStreamingSQL"
 *     --master local[4]
 *     target/log-analyzer-1.0.jar
 */
public class LogAnalyzerStreamingSQL {
  // Stats will be computed for the last window length of time.
  private static final Duration WINDOW_LENGTH = new Duration(30 * 1000);
  // Stats will be computed every slide interval time.
  private static final Duration SLIDE_INTERVAL = new Duration(10 * 1000);

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Log Analyzer Streaming SQL");

    // Note: Only one Spark Context is created from the conf, the rest
    //       are created from the original Spark context.
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaStreamingContext jssc = new JavaStreamingContext(sc,
        SLIDE_INTERVAL);  // This sets the update window to be every 10 seconds.
    final SQLContext sqlContext = new SQLContext(sc);

    JavaReceiverInputDStream<String> logDataDStream =
        jssc.socketTextStream("localhost", 9999);

// A DStream of Apache Access Logs.
    JavaDStream<ApacheAccessLog> accessLogDStream =
        logDataDStream.map(Functions.PARSE_LOG_LINE);

    // Splits the accessLogDStream into a dstream of time windowed rdd's of apache access logs.
    JavaDStream<ApacheAccessLog> windowDStream =
        accessLogDStream.window(WINDOW_LENGTH, SLIDE_INTERVAL);

    windowDStream.foreachRDD(
        new Function<JavaRDD<ApacheAccessLog>, Void>() {
          @Override
          public Void call(JavaRDD<ApacheAccessLog> accessLogs) {
            if (accessLogs.count() == 0) {
              System.out.println("No access logs in this time interval");
              return null;
            }

            // *** Note that this is code copied verbatim from LogAnalyzerSQL.java.
            DataFrame sqlDataFrame = sqlContext.createDataFrame(
                accessLogs, ApacheAccessLog.class);
            sqlDataFrame.registerTempTable("logs");
            sqlContext.cacheTable("logs");

            // Calculate statistics based on the content size.
            Tuple4<Long, Long, Long, Long> contentSizeStats =
                sqlContext.sql("SELECT SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize) FROM logs")
                    .javaRDD()
                    .map(new Function<Row, Tuple4<Long, Long, Long, Long>>() {
                      @Override
                      public Tuple4<Long, Long, Long, Long> call(Row row)
                          throws Exception {
                        return new Tuple4<Long, Long, Long, Long>(row.getLong(0),
                            row.getLong(1), row.getLong(2), row.getLong(3));
                      }
                    })
                    .first();
            System.out.println(String.format(
                "Content Size Avg: %s, Min: %s, Max: %s",
                contentSizeStats._1() / contentSizeStats._2(),
                contentSizeStats._3(),
                contentSizeStats._4()));

            // Compute Response Code to Count.
            List<Tuple2<Integer, Long>> responseCodeToCount = sqlContext
                .sql("SELECT responseCode, COUNT(*) FROM logs GROUP BY responseCode LIMIT 100")
                .javaRDD()
                .mapToPair(new PairFunction<Row, Integer, Long>() {
                  @Override
                  public Tuple2<Integer, Long> call(Row row) throws Exception {
                    return new Tuple2<Integer, Long>(row.getInt(0), row.getLong(1));
                  }
                })
                .collect();
            System.out.println(
                String.format("Response code counts: %s", responseCodeToCount));

            // Any IPAddress that has accessed the server more than 10 times.
            List<String> ipAddresses = sqlContext
                .sql("SELECT ipAddress, COUNT(*) AS total FROM logs GROUP BY ipAddress HAVING total > 10 LIMIT 100")
                .javaRDD()
                .map(new Function<Row, String>() {
                  @Override
                  public String call(Row row) throws Exception {
                    return row.getString(0);
                  }
                })
                .collect();  // Take only 100 in case this is a super large data set.
            System.out.println(
                String.format("IPAddresses > 10 times: %s", ipAddresses));

            // Top Endpoints.
            List<Tuple2<String, Long>> topEndpoints = sqlContext
                .sql("SELECT endpoint, COUNT(*) AS total FROM logs GROUP BY endpoint ORDER BY total DESC LIMIT 10")
                .javaRDD()
                .map(new Function<Row, Tuple2<String, Long>>() {
                  @Override
                  public Tuple2<String, Long> call(Row row) throws Exception {
                    return new Tuple2<String, Long>(
                        row.getString(0), row.getLong(1));
                  }
                })
                .collect();
            System.out.println(
                String.format("Top Endpoints: %s", topEndpoints));

            return null;
          }
        });

    // Start the streaming server.
    jssc.start();              // Start the computation
    jssc.awaitTermination();   // Wait for the computation to terminate
  }
}
