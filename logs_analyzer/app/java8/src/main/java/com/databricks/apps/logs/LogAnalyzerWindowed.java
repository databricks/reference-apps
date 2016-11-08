package com.databricks.apps.logs;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import scala.Tuple2;
import scala.Tuple4;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.api.java.JavaDStream;

public class LogAnalyzerWindowed implements Serializable {
  private LogStatistics logStatistics;

  public void processAccessLogs(JavaDStream<ApacheAccessLog> accessLogsDStream) {
    JavaDStream<ApacheAccessLog> windowDStream = accessLogsDStream.window(
        Flags.getInstance().getWindowLength(),
        Flags.getInstance().getSlideInterval());
    windowDStream.foreachRDD(accessLogs -> {
      if (accessLogs.count() > 0) {
        Tuple4<Long, Long, Long, Long> contentSizeStats =
            Functions.contentSizeStats(accessLogs);

        List<Tuple2<Integer, Long>> responseCodeToCount =
            Functions.responseCodeCount(accessLogs)
                .take(100);

        JavaPairRDD<String, Long> ipAddressCounts =
            Functions.ipAddressCount(accessLogs);
        List<String> ipAddresses = Functions.filterIPAddress(ipAddressCounts)
            .take(100);

        List<Tuple2<String, Long>> topEndpoints =
            Functions.endpointCount(accessLogs)
                .top(10, new Functions.ValueComparator<>(Comparator.<Long>naturalOrder()));

        logStatistics = new LogStatistics(contentSizeStats, responseCodeToCount,
            ipAddresses, topEndpoints);
      } else {
        logStatistics = new LogStatistics(new Tuple4<>(0L, 0L, 0L, 0L),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList());
      }
    });
  }

  public LogStatistics getLogStatistics() {
    return logStatistics;
  }
}
