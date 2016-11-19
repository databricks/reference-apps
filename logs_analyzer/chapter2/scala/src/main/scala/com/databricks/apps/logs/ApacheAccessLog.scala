package com.databricks.apps.logs

import java.io.IOException

/**
  * An entry of Apache access log.
  */
// TODO Unify across all Scala chapters.
case class ApacheAccessLog(ipAddress: String,
                           clientIdentd: String,
                           userId: String,
                           dateTime: String,
                           method: String,
                           endpoint: String,
                           protocol: String,
                           responseCode: Int,
                           contentSize: Long) {
}

object ApacheAccessLog {
  val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)""".r

  /**
    * Parse log entry from a string.
    * @param log A string, typically a line from a log file
    * @return An entry of Apache access log
    * @throws IOException Unable to parse the string
    */
  def parseLogLine(log: String): ApacheAccessLog = {
    val apacheAccessLog: ApacheAccessLog = log match {
      case PATTERN(ipAddress, clientIdentd, userId, dateTime, method, endpoint, protocol, responseCode, contentSize)
      => ApacheAccessLog(ipAddress, clientIdentd, userId, dateTime, method, endpoint, protocol, responseCode.toInt,
        contentSize.toLong)
      case _ => throw new RuntimeException(s"""Cannot parse log line: $log""")
    }
    return apacheAccessLog
  }
}
