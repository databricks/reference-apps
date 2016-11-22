package com.databricks.apps.logs

import java.util.concurrent.atomic.AtomicLong

import scala.math._

import org.apache.spark.streaming.dstream.DStream

/**
  * Collects total statistics from the moment application started.
  */
class LogAnalyzerTotal extends AnalyzeFunctions with Serializable {
  val runningCount = new AtomicLong(0)
  val runningSum = new AtomicLong(0)
  val runningMin = new AtomicLong(0)
  val runningMax = new AtomicLong(0)
  var currentResponseCodeCounts: Option[Array[(Int, Long)]] = None
  var currentIPAddresses: Option[Array[String]] = None
  var currentTopEndpoints: Option[Array[(String, Long)]] = None

  def processAccessLogs(accessLogsDStream: DStream[ApacheAccessLog]): Unit = {
    // Calculate statistics based on the content size, and update the static variables to track this.
    accessLogsDStream.foreachRDD(accessLogs => {
      val stats = contentSizeStats(accessLogs)
      stats.foreach(s => {
        runningCount.getAndAdd(s._1)
        runningSum.getAndAdd(s._2)
        runningMin.set(min(runningMin.get(), s._3))
        runningMax.set(max(runningMax.get(), s._4))
      })
    })

    // A DStream of Response Code Counts;
    val responseCodeCountDStream = accessLogsDStream
      .transform(responseCodeCount)
      .updateStateByKey(computeRunningSum)
    responseCodeCountDStream.foreachRDD(rdd => currentResponseCodeCounts = Some(rdd.take(100)))

    // A DStream of ipAddressCounts.
    val ipAddressesDStream = accessLogsDStream
      .transform(ipAddressCount)
      .updateStateByKey(computeRunningSum)
      .transform(filterIPAddress)
    ipAddressesDStream.foreachRDD(rdd => currentIPAddresses = Some(rdd.take(100)))

    // A DStream of endpoint to count.
    val endpointCountsDStream = accessLogsDStream
      .transform(endpointCount)
      .updateStateByKey(computeRunningSum)
    endpointCountsDStream
      .foreachRDD(rdd => currentTopEndpoints = Some(rdd.top(10)(Ordering.by[(String, Long), Long](_._2))))
  }

  def getLogStatistics: LogStatistics = {
    LogStatistics(
      (runningCount.get(),
        runningSum.get(),
        runningMin.get(),
        runningMax.get()),
      currentResponseCodeCounts.getOrElse(Array.empty),
      currentIPAddresses.getOrElse(Array.empty),
      currentTopEndpoints.getOrElse(Array.empty))
  }
}
