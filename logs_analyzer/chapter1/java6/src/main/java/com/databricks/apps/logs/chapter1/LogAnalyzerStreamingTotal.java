package com.databricks.apps.logs.chapter1;

import com.databricks.apps.logs.ApacheAccessLog;
import com.databricks.apps.logs.Functions;
import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This LogAnalyzerStreaming program reads the localhost 9999 socket
 * for incoming Apache access logs and computes the statistics on those logs
 * every 10 seconds.  It computes the statistics for data from all of time.
 *
 * To feed the new lines of some logfile into a socket, run this command:
 *   % tail -f [[YOUR_LOG_FILE]] | nc -lk 9999
 *
 * If you don't have a live log file that is being written to,
 * you can add test lines using this command:
 *   % cat data/apache.accesslog >> [[YOUR_LOG_FILE]]
 *
 * Example command to run:
 * %  ${YOUR_SPARK_HOME}/bin/spark-submit
 *     --class "com.databricks.apps.logs.chapter1.LogAnalyzerStreamingTotal"
 *     --master local[4]
 *     target/log-analyzer-1.0.jar
 */
public class LogAnalyzerStreamingTotal {
  private static Function2<List<Long>, Optional<Long>, Optional<Long>>
      COMPUTE_RUNNING_SUM =
      new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
        @Override
        public Optional<Long> call(List<Long> nums, Optional<Long> current)
            throws Exception {
          long sum = current.or(0L);
          for (long i : nums) {
            sum += i;
          }
          return Optional.of(sum);
        }
      };

  // These static variables stores the running content size values.
  private static final AtomicLong runningCount = new AtomicLong(0);
  private static final AtomicLong runningSum = new AtomicLong(0);
  private static final AtomicLong runningMin = new AtomicLong(Long.MAX_VALUE);
  private static final AtomicLong runningMax = new AtomicLong(Long.MIN_VALUE);

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Log Analyzer Streaming Total");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaStreamingContext jssc = new JavaStreamingContext(sc,
        new Duration(10000));  // This sets the update window to be every 10 seconds.

    // Checkpointing must be enabled to use the updateStateByKey function.
    jssc.checkpoint("/tmp/log-analyzer-streaming");

    JavaReceiverInputDStream<String> logDataDStream =
        jssc.socketTextStream("localhost", 9999);

    JavaDStream<ApacheAccessLog> accessLogDStream = logDataDStream.map(Functions.PARSE_LOG_LINE);

    // Calculate statistics based on the content size, and update the static variables to track this.
    JavaDStream<Long> contentSizeDStream = accessLogDStream.map(Functions.GET_CONTENT_SIZE);
    contentSizeDStream.foreachRDD(new Function<JavaRDD<Long>, Void>() {
      @Override
      public Void call(JavaRDD<Long> rdd) throws Exception {
        if (rdd.count() > 0) {
          runningSum.getAndAdd(rdd.reduce(Functions.SUM_REDUCER));
          runningCount.getAndAdd(rdd.count());
          runningMin.set(Math.min(runningMin.get(),
              rdd.min(Functions.LONG_NATURAL_ORDER_COMPARATOR)));
          runningMax.set(Math.max(runningMax.get(),
              rdd.max(Functions.LONG_NATURAL_ORDER_COMPARATOR)));
          System.out.print("Content Size Avg: " +
              runningSum.get() / runningCount.get());
          System.out.print(", Min: " + runningMin.get());
          System.out.println(", Max: " + runningMax.get());
        }
        return null;
      }
    });

    // Compute Response Code to Count.
    // Note the use of updateStateByKey.
    JavaPairDStream<Integer, Long> responseCodeCountDStream = accessLogDStream
        .mapToPair(Functions.GET_RESPONSE_CODE)
        .reduceByKey(Functions.SUM_REDUCER)
        .updateStateByKey(COMPUTE_RUNNING_SUM);
    responseCodeCountDStream.foreachRDD(
        new Function<JavaPairRDD<Integer, Long>, Void>() {
          @Override
          public Void call(JavaPairRDD<Integer, Long> rdd) throws Exception {
            System.out.println("Response code counts: " + rdd.take(100));
            return null;
          }
        });

    // A DStream of ipAddresses accessed > 10 times.
    JavaDStream<String> ipAddressesDStream = accessLogDStream
        .mapToPair(Functions.GET_IP_ADDRESS)
        .reduceByKey(Functions.SUM_REDUCER)
        .updateStateByKey(COMPUTE_RUNNING_SUM)
        .filter(Functions.FILTER_GREATER_10)
        .map(Functions.GET_TUPLE_FIRST);
    ipAddressesDStream.foreachRDD(
        new Function<JavaRDD<String>, Void>() {
          @Override
          public Void call(JavaRDD<String> rdd) throws Exception {
            List<String> ipAddresses = rdd.take(100);
            System.out.println("All IPAddresses > 10 times: " + ipAddresses);
            return null;
          }
        });

    // A DStream of endpoint to count.
    JavaPairDStream<String, Long> endpointCountsDStream = accessLogDStream
        .mapToPair(Functions.GET_ENDPOINT)
        .reduceByKey(Functions.SUM_REDUCER)
        .updateStateByKey(COMPUTE_RUNNING_SUM);
    endpointCountsDStream.foreachRDD(
        new Function<JavaPairRDD<String, Long>, Void>() {
          @Override
          public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
            List<Tuple2<String, Long>> topEndpoints =
                rdd.takeOrdered(10, new Functions.ValueComparator<String, Long>(
                    Functions.LONG_NATURAL_ORDER_COMPARATOR));
            System.out.println("Top Endpoints: " + topEndpoints);
            return null;
          }
        });

    // Start the streaming server.
    jssc.start();              // Start the computation
    jssc.awaitTermination();   // Wait for the computation to terminate
  }
}
