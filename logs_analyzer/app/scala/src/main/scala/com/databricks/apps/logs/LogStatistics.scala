package com.databricks.apps.logs

/** Aggregates log statistics. */
case class LogStatistics(contentSizeStats: (Long, Long, Long, Long),
                         responseCodeToCount: Map[Int, Long],
                         ipAddresses: Array[String],
                         topEndpoints: Map[String, Long]) extends Serializable

object LogStatistics {
  val EMPTY_LOG_STATISTICS = LogStatistics((0, 0, 0, 0),
    Map.empty,
    Array.empty,
    Map.empty)
}
