package com.databricks.apps.logs

import org.apache.spark.rdd.RDD

/**
  * Functions to analyze Apache logs.
  */
trait AnalyzeFunctions {

  def computeRunningSum = (nums: Seq[Long], current: Option[Long]) => Some(current.getOrElse(0L) + nums.sum)

  def contentSizeStats = (accessLogs: RDD[ApacheAccessLog]) => {
    val contentSizes = accessLogs.map(_.contentSize).cache()
    if (contentSizes.count() > 0)
      Some((contentSizes.count(), contentSizes.reduce(_ + _), contentSizes.min(), contentSizes.max()))
    else
      None
  }

  def responseCodeCount = (accessLogs: RDD[ApacheAccessLog]) => accessLogs.map(_.responseCode -> 1L).reduceByKey(_ + _)

  def ipAddressCount = (accessLogs: RDD[ApacheAccessLog]) => accessLogs.map(_.ipAddress -> 1L).reduceByKey(_ + _)

  def filterIPAddress = (ipAddressCount: RDD[(String, Long)]) => ipAddressCount.filter(_._2 > 10).map(_._1)

  def endpointCount = (accessLogs: RDD[ApacheAccessLog]) => accessLogs.map(_.endpoint -> 1L).reduceByKey(_ + _)
}
