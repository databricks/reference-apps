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
import scala.Tuple4;

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
 *     --class "com.databricks.apps.logs.chapter1.LogAnalyzerStreamingTotalRefactored"
 *     --master local[4]
 *     target/log-analyzer-1.0.jar
 */
public class LogAnalyzerStreamingTotalRefactored {
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

  // These functions below that could be shared with the batch library.
  public static Tuple4<Long, Long, Long, Long> contentSizeStats(
      JavaRDD<ApacheAccessLog> accessLogRDD) {
    JavaRDD<Long> contentSizes =
        accessLogRDD.map(Functions.GET_CONTENT_SIZE).cache();
    return new Tuple4<Long, Long, Long, Long>(contentSizes.count(),
        contentSizes.reduce(Functions.SUM_REDUCER),
        contentSizes.min(Functions.LONG_NATURAL_ORDER_COMPARATOR),
        contentSizes.max(Functions.LONG_NATURAL_ORDER_COMPARATOR));
  }

  public static JavaPairRDD<Integer, Long> responseCodeCount(
      JavaRDD<ApacheAccessLog> accessLogRDD) {
    return accessLogRDD
        .mapToPair(Functions.GET_RESPONSE_CODE)
        .reduceByKey(Functions.SUM_REDUCER);
  }

  public static JavaPairRDD<String, Long> ipAddressCount(
      JavaRDD<ApacheAccessLog> accessLogRDD) {
    return accessLogRDD
        .mapToPair(Functions.GET_IP_ADDRESS)
        .reduceByKey(Functions.SUM_REDUCER);
  }

  public static JavaRDD<String> filterIPAddress(
      JavaPairRDD<String, Long> ipAddressCount) {
    return ipAddressCount
        .filter(Functions.FILTER_GREATER_10)
        .map(Functions.GET_TUPLE_FIRST);
  }

  public static JavaPairRDD<String, Long> endpointCount(
      JavaRDD<ApacheAccessLog> accessLogRDD) {
    return accessLogRDD
        .mapToPair(Functions.GET_ENDPOINT)
        .reduceByKey(Functions.SUM_REDUCER);
  }

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Log Analyzer Streaming Total");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaStreamingContext jssc = new JavaStreamingContext(sc,
        new Duration(10000));  // This sets the update window to be every 10 seconds.

    // Checkpointing must be enabled to use the updateStateByKey function.
    jssc.checkpoint("/tmp/log-analyzer-streaming");

    JavaReceiverInputDStream<String> logDataDStream =
        jssc.socketTextStream("localhost", 9999);

    final JavaDStream<ApacheAccessLog> accessLogDStream =
        logDataDStream.map(Functions.PARSE_LOG_LINE).cache();

    // Calculate statistics based on the content size, and update the static variables to track this.
    accessLogDStream.foreachRDD(new Function<JavaRDD<ApacheAccessLog>, Void>() {
      @Override
      public Void call(JavaRDD<ApacheAccessLog> accessLogRDD) throws Exception {
        if (accessLogRDD.count() > 0) {
          Tuple4<Long, Long, Long, Long> contentSizeStats =
              contentSizeStats(accessLogRDD);
          runningCount.getAndAdd(contentSizeStats._1());
          runningSum.getAndAdd(contentSizeStats._2());
          runningMin.set(Math.min(runningMin.get(), contentSizeStats._3()));
          runningMax.set(Math.max(runningMax.get(), contentSizeStats._4()));

          System.out.print("Content Size Avg: " + runningSum.get() / runningCount.get());
          System.out.print(", Min: " + runningMin.get());
          System.out.println(", Max: " + runningMax.get());
        }

        return null;
      }
    });

    // Compute Response Code to Count.
    // Notice the user transformToPair to produce the a DStream of
    // response code counts, and then updateStateByKey to accumulate
    // the response code counts for all of time.
    JavaPairDStream<Integer, Long> responseCodeCountDStream = accessLogDStream
        .transformToPair(new Function<JavaRDD<ApacheAccessLog>,
            JavaPairRDD<Integer, Long>>() {
          @Override
          public JavaPairRDD<Integer, Long> call(
              JavaRDD<ApacheAccessLog> accessLogJavaRDD) throws Exception {
            return responseCodeCount(accessLogJavaRDD);
          }
        });
    JavaPairDStream<Integer, Long> cumulativeResponseCodeCountDStream =
        responseCodeCountDStream.updateStateByKey(COMPUTE_RUNNING_SUM);
    cumulativeResponseCodeCountDStream.foreachRDD(
        new Function<JavaPairRDD<Integer, Long>, Void>() {
          @Override
          public Void call(JavaPairRDD<Integer, Long> rdd) throws Exception {
            System.out.println("Response code counts: " + rdd.take(100));
            return null;
          }
        });

    // A DStream of ipAddresses accessed > 10 times.
    JavaDStream<String> ipAddressesDStream = accessLogDStream
        .transformToPair(new Function<JavaRDD<ApacheAccessLog>,
            JavaPairRDD<String, Long>>() {
          @Override
          public JavaPairRDD<String, Long> call(
              JavaRDD<ApacheAccessLog> accessLogJavaRDD) throws Exception {
            return ipAddressCount(accessLogJavaRDD);
          }
        })
        .updateStateByKey(COMPUTE_RUNNING_SUM)
        .transform(new Function<JavaPairRDD<String, Long>,
            JavaRDD<String>>() {
          @Override
          public JavaRDD<String> call(JavaPairRDD<String, Long> rdd)
              throws Exception {
            return filterIPAddress(rdd);
          }
        });
    ipAddressesDStream.foreachRDD(
        new Function<JavaRDD<String>, Void>() {
          @Override
          public Void call(JavaRDD<String> ipAddressRdd) throws Exception {
            List<String> ipAddresses = ipAddressRdd.take(100);
            System.out.println("All IPAddresses > 10 times: " + ipAddresses);
            return null;
          }
        });

    // A DStream of endpoint to count.
    JavaPairDStream<String, Long> endpointCountsDStream = accessLogDStream
        .transformToPair(new Function<JavaRDD<ApacheAccessLog>,
            JavaPairRDD<String, Long>>() {
          @Override
          public JavaPairRDD<String, Long> call(
              JavaRDD<ApacheAccessLog> accessLogJavaRDD) throws Exception {
            return endpointCount(accessLogJavaRDD);
          }
        })
        .updateStateByKey(COMPUTE_RUNNING_SUM);
    endpointCountsDStream.foreachRDD(
        new Function<JavaPairRDD<String, Long>, Void>() {
          @Override
          public Void call(JavaPairRDD<String, Long> rdd)
              throws Exception {
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

