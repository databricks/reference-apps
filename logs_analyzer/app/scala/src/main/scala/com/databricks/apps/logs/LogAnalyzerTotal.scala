package com.databricks.apps.logs

import java.util.concurrent.atomic.AtomicLong

import scala.math._

import org.apache.spark.streaming.dstream.DStream

/** Collects total statistics from the moment application started. */
class LogAnalyzerTotal extends AnalyzeFunctions with Serializable {
  val runningCount = new AtomicLong(0)
  val runningSum = new AtomicLong(0)
  val runningMin = new AtomicLong(0)
  val runningMax = new AtomicLong(0)
  var currentResponseCodeCounts: Map[Int, Long] = Map.empty
  var currentIPAddresses: Array[String] = Array.empty
  var currentTopEndpoints: Map[String, Long] = Map.empty

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
    val responseCodeCountDStream: DStream[(Int, Long)] = accessLogsDStream
      .transform(responseCodeCount)
      .updateStateByKey(computeRunningSum)
    responseCodeCountDStream.foreachRDD(rdd => currentResponseCodeCounts = rdd.take(100).toMap)

    // A DStream of ipAddressCounts.
    val ipAddressesDStream: DStream[String] = accessLogsDStream
      .transform(ipAddressCount)
      .updateStateByKey(computeRunningSum)
      .transform(filterIPAddress)
    ipAddressesDStream.foreachRDD(rdd => currentIPAddresses = rdd.take(100))

    // A DStream of endpoint to count.
    val endpointCountsDStream: DStream[(String, Long)] = accessLogsDStream
      .transform(endpointCount)
      .updateStateByKey(computeRunningSum)
    endpointCountsDStream
      .foreachRDD(rdd => currentTopEndpoints = rdd.top(10)(Ordering.by[(String, Long), Long](_._2)).toMap)
  }

  def getLogStatistics: LogStatistics = {
    LogStatistics(
      (runningCount.get(),
        runningSum.get(),
        runningMin.get(),
        runningMax.get()),
      currentResponseCodeCounts,
      currentIPAddresses,
      currentTopEndpoints)
  }
}
