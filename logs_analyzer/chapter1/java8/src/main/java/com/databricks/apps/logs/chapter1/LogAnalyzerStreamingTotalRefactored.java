package com.databricks.apps.logs.chapter1;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import scala.Tuple2;
import scala.Tuple4;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.databricks.apps.logs.ApacheAccessLog;

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
 *   % cat data/apache.access.log >> [[YOUR_LOG_FILE]]
 *
 * Example command to run:
 * %  ${YOUR_SPARK_HOME}/bin/spark-submit
 *     --class "com.databricks.apps.logs.chapter1.LogAnalyzerStreamingTotalRefactored"
 *     --master local[*]
 *     target/log-analyzer-2.0.jar
 *
 * On another console, run the shell script that emulates network stream
 * by periodically sending portions of the sample log file to a network socket:
 * % cd ../../data
 * % ./stream.sh apache.access.log
 */
public class LogAnalyzerStreamingTotalRefactored {
  private static final Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;

  private static class ValueComparator<K, V>
     implements Comparator<Tuple2<K, V>>, Serializable {
    private final Comparator<V> comparator;

    ValueComparator(Comparator<V> comparator) {
      this.comparator = comparator;
    }

    @Override
    public int compare(Tuple2<K, V> o1, Tuple2<K, V> o2) {
      return comparator.compare(o1._2(), o2._2());
    }
  }

  private static final Function2<List<Long>, Optional<Long>, Optional<Long>>
     COMPUTE_RUNNING_SUM = (nums, current) -> {
    long sum = current.or(0L);
    for (long i : nums) {
      sum += i;
    }
    return Optional.of(sum);
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
       accessLogRDD.map(ApacheAccessLog::getContentSize);
    return new Tuple4<>(contentSizes.count(), contentSizes.reduce(SUM_REDUCER),
       contentSizes.min(Comparator.naturalOrder()),
       contentSizes.max(Comparator.naturalOrder()));
  }

  public static JavaPairRDD<Integer, Long> responseCodeCount(
     JavaRDD<ApacheAccessLog> accessLogRDD) {
    return accessLogRDD
       .mapToPair(s -> new Tuple2<>(s.getResponseCode(), 1L))
       .reduceByKey(SUM_REDUCER);
  }

  public static JavaPairRDD<String, Long> ipAddressCount(
     JavaRDD<ApacheAccessLog> accessLogRDD) {
    return accessLogRDD
       .mapToPair(log -> new Tuple2<>(log.getIpAddress(), 1L))
       .reduceByKey(SUM_REDUCER);
  }

  public static JavaRDD<String> filterIPAddress(
     JavaPairRDD<String, Long> ipAddressCount) {
    return ipAddressCount
       .filter(tuple -> tuple._2() > 10)
       .map(Tuple2::_1);
  }

  public static JavaPairRDD<String, Long> endpointCount(
     JavaRDD<ApacheAccessLog> accessLogRDD) {
    return accessLogRDD
       .mapToPair(log -> new Tuple2<>(log.getEndpoint(), 1L))
       .reduceByKey(SUM_REDUCER);
  }

  public static void main(String[] args) throws InterruptedException {
    SparkConf conf = new SparkConf().setAppName("Log Analyzer Streaming Total");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaStreamingContext jssc = new JavaStreamingContext(sc,
       new Duration(10000));  // This sets the update window to be every 10 seconds.

    // Checkpointing must be enabled to use the updateStateByKey function.
    jssc.checkpoint("checkpoints-log-analyzer-streaming-total");

    JavaReceiverInputDStream<String> logDataDStream =
       jssc.socketTextStream("localhost", 9999);

    JavaDStream<ApacheAccessLog> accessLogDStream =
       logDataDStream.map(ApacheAccessLog::parseFromLogLine);

    // Calculate statistics based on the content size, and update the static variables to track this.
    accessLogDStream.foreachRDD(accessLogRDD -> {
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
       }
    );

    // Compute Response Code to Count.
    // Notice the user transformToPair to produce the a DStream of
    // response code counts, and then updateStateByKey to accumulate
    // the response code counts for all of time.
    JavaPairDStream<Integer, Long> responseCodeCountDStream = accessLogDStream
       .transformToPair(LogAnalyzerStreamingTotalRefactored::responseCodeCount);
    JavaPairDStream<Integer, Long> cumulativeResponseCodeCountDStream =
       responseCodeCountDStream.updateStateByKey(COMPUTE_RUNNING_SUM);
    cumulativeResponseCodeCountDStream.foreachRDD(rdd -> {
      System.out.println("Response code counts: " + rdd.take(100));
    });

    // A DStream of ipAddresses accessed > 10 times.
    JavaDStream<String> ipAddressesDStream = accessLogDStream
       .transformToPair(LogAnalyzerStreamingTotalRefactored::ipAddressCount)
       .updateStateByKey(COMPUTE_RUNNING_SUM)
       .transform(LogAnalyzerStreamingTotalRefactored::filterIPAddress);
    ipAddressesDStream.foreachRDD(rdd -> {
      List<String> ipAddresses = rdd.take(100);
      System.out.println("All IPAddresses > 10 times: " + ipAddresses);
    });

    // A DStream of endpoint to count.
    JavaPairDStream<String, Long> endpointCountsDStream = accessLogDStream
       .transformToPair(LogAnalyzerStreamingTotalRefactored::endpointCount)
       .updateStateByKey(COMPUTE_RUNNING_SUM);
    endpointCountsDStream.foreachRDD(rdd -> {
      List<Tuple2<String, Long>> topEndpoints =
         rdd.top(10, new ValueComparator<>(Comparator.<Long>naturalOrder()));
      System.out.println("Top Endpoints: " + topEndpoints);
    });

    // Start the streaming server.
    jssc.start();              // Start the computation
    jssc.awaitTermination();   // Wait for the computation to terminate
  }
}
