package com.databricks.apps.logs

import scala.math.Ordering

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

/**
  * Collects statistics within a window.
  */
class LogAnalyzerWindowed(val windowLength: Int, val slideInterval: Int) extends AnalyzeFunctions with Serializable {
  import LogStatistics.EMPTY

  var logStatistics = EMPTY

  def processAccessLogs(accessLogsDStream: DStream[ApacheAccessLog]): Unit = {
    val windowDStream = accessLogsDStream.window(Seconds(windowLength), Seconds(slideInterval))
    windowDStream.foreachRDD(accessLogs => {
      if (accessLogs.count() == 0) {
        logStatistics = EMPTY
      } else {
        logStatistics = LogStatistics(contentSizeStats(accessLogs).get,
          responseCodeCount(accessLogs).take(100),
          filterIPAddress(ipAddressCount(accessLogs)).take(100),
          endpointCount(accessLogs).top(10)(Ordering.by[(String, Long), Long](_._2)))
      }
    })
  }

  def getLogStatistics: LogStatistics = logStatistics
}
