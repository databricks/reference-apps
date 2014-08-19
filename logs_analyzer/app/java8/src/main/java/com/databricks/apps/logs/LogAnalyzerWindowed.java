package com.databricks.apps.logs;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaDStream;
import scala.Tuple2;
import scala.Tuple4;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

public class LogAnalyzerWindowed implements Serializable {
  private LogStatistics logStatistics;

  public void processAccessLogs(JavaDStream<ApacheAccessLog> accessLogsDStream) {

    JavaDStream<ApacheAccessLog> windowDStream = accessLogsDStream.window(
        LogAnalyzerAppMain.Flags.getInstance().getWindowLength(),
        LogAnalyzerAppMain.Flags.getInstance().getSlideInterval());
    windowDStream.foreachRDD(accessLogs -> {
      JavaRDD<Long> contentSizes =
          accessLogs.map(ApacheAccessLog::getContentSize).cache();

      long count = contentSizes.count();
      if (count == 0) {
        logStatistics = LogStatistics.EMPTY_LOG_STATISTICS;
        return null;
      }

      Tuple4<Long, Long, Long, Long> contentSizeStats = new Tuple4<>(
          contentSizes.reduce(Functions.SUM_REDUCER),
          count,
          contentSizes.min(Comparator.naturalOrder()),
          contentSizes.max(Comparator.naturalOrder()));

      List<Tuple2<Integer, Long>> responseCodeToCount = accessLogs.mapToPair(
          log -> new Tuple2<>(log.getResponseCode(), 1L))
          .reduceByKey(Functions.SUM_REDUCER)
          .take(100);

      List<String> ipAddresses = accessLogs
          .mapToPair(log -> new Tuple2<>(log.getIpAddress(), 1L))
          .reduceByKey(Functions.SUM_REDUCER)
          .filter(tuple -> tuple._2() > 10)
          .map(Tuple2::_1)
          .take(100);

      List<Tuple2<String, Long>> topEndpoints = accessLogs
          .mapToPair(log -> new Tuple2<>(log.getEndpoint(), 1L))
          .reduceByKey(Functions.SUM_REDUCER)
          .top(10, new Functions.ValueComparator<>(Comparator.<Long>naturalOrder()));

      logStatistics = new LogStatistics(contentSizeStats, responseCodeToCount,
          ipAddresses, topEndpoints);

      return null;
    });
  }

  public LogStatistics getLogStatistics() {
    return logStatistics;
  }
}
