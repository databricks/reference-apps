package com.databricks.apps.logs

/**
  * Aggregates log statistics.
  */
case class LogStatistics(contentSizeStats: (Long, Long, Long, Long),
                         responseCodeToCount: Array[(Int, Long)],
                         ipAddresses: Array[String],
                         topEndpoints: Array[(String, Long)]) extends Serializable
object LogStatistics {
  val EMPTY = LogStatistics((0, 0, 0, 0),
    Array.empty,
    Array.empty,
    Array.empty)
}
