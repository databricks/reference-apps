package com.databricks.apps.logs

/** Aggregates log statistics. */
case class LogStatistics(contentSizeStats: (Long, Long, Long, Long),
                         responseCodeToCount: Array[(Int, Long)],
                         ipAddresses: Array[String],
                         topEndpoints: Array[(String, Long)]) {

  def printToStandardOut() : Unit = {
    if (contentSizeStats._2 > 0) {
      println("Content Size Avg: %s, Min: %s, Max: %s".format(
        contentSizeStats._1 / contentSizeStats._2,
        contentSizeStats._3,
        contentSizeStats._4))
    } else {
      println("Content Size Avg: -, Min: -, Max: -")
    }
    println(s"""Response code counts: ${responseCodeToCount.mkString("[", ",", "]")}""")
    println(s"""IPAddresses > 10 times: ${ipAddresses.mkString("[", ",", "]")}""")
    println(s"""Top Endpoints: ${topEndpoints.mkString("[", ",", "]")}""")
  }
}
