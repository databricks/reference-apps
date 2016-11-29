package com.databricks.apps.logs

import scala.math.Ordering

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

/** Collects statistics within a window. */
class LogAnalyzerWindowed(val windowLength: Long, val slideInterval: Long) extends AnalyzeFunctions with Serializable {

  import LogStatistics.EMPTY_LOG_STATISTICS

  var logStatistics = EMPTY_LOG_STATISTICS

  def processAccessLogs(accessLogsDStream: DStream[ApacheAccessLog]): Unit = {
    val windowDStream: DStream[ApacheAccessLog] = accessLogsDStream
      .window(Seconds(windowLength), Seconds(slideInterval))
    windowDStream.foreachRDD(accessLogs => {
      if (accessLogs.count() == 0) {
        logStatistics = EMPTY_LOG_STATISTICS
      } else {
        logStatistics = LogStatistics(contentSizeStats(accessLogs).get,
          responseCodeCount(accessLogs).take(100).toMap,
          filterIPAddress(ipAddressCount(accessLogs)).take(100),
          endpointCount(accessLogs).top(10)(Ordering.by[(String, Long), Long](_._2)).toMap)
      }
    })
  }

  def getLogStatistics: LogStatistics = logStatistics
}
