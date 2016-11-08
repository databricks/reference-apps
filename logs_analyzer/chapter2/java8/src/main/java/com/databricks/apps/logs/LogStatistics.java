package com.databricks.apps.logs;

import java.util.List;

import scala.Tuple2;
import scala.Tuple4;

public class LogStatistics {
  private Tuple4<Long, Long, Long, Long> contentSizeStats;
  private List<Tuple2<Integer, Long>> responseCodeToCount;
  private List<String> ipAddresses;
  private List<Tuple2<String, Long>> topEndpoints;

  public Tuple4<Long, Long, Long, Long> getContentSizeStats() {
    return contentSizeStats;
  }

  public void setContentSizeStats(Tuple4<Long, Long, Long, Long> contentSizeStats) {
    this.contentSizeStats = contentSizeStats;
  }

  public List<Tuple2<Integer, Long>> getResponseCodeToCount() {
    return responseCodeToCount;
  }

  public void setResponseCodeToCount(List<Tuple2<Integer, Long>> responseCodeToCount) {
    this.responseCodeToCount = responseCodeToCount;
  }

  public List<String> getIpAddresses() {
    return ipAddresses;
  }

  public void setIpAddresses(List<String> ipAddresses) {
    this.ipAddresses = ipAddresses;
  }

  public List<Tuple2<String, Long>> getTopEndpoints() {
    return topEndpoints;
  }

  public void setTopEndpoints(List<Tuple2<String, Long>> topEndpoints) {
    this.topEndpoints = topEndpoints;
  }

  public void clear() {
    contentSizeStats = null;
    responseCodeToCount = null;
    ipAddresses = null;
    topEndpoints = null;
  }

  public void printToStandardOut() {
    if (contentSizeStats._2() > 0) {
      System.out.println(String.format("Content Size Avg: %s, Min: %s, Max: %s",
          contentSizeStats._1() / contentSizeStats._2(),
          contentSizeStats._3(),
          contentSizeStats._4()));
    } else {
      System.out.println("Content Size Avg: -, Min: -, Max: -");
    }

    System.out.println(String.format("Response code counts: %s", responseCodeToCount));

    System.out.println(String.format("IPAddresses > 10 times: %s", ipAddresses));

    System.out.println(String.format("Top Endpoints: %s", topEndpoints));
  }
}
