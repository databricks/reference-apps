package com.databricks.apps.logs

import org.scalatest.{FlatSpec, Matchers}

class ApacheAccessLogTest extends FlatSpec with Matchers {

  "parseLogLine" should "return empty value for null argument" in {
    assert(ApacheAccessLog.parseLogLine(null).isEmpty)
  }

  it should "return empty value for empty string argument" in {
    assert(ApacheAccessLog.parseLogLine("").isEmpty)
  }

  it should "correctly parse all log entry parameters" in {
    val apacheAccessLog = ApacheAccessLog
      .parseLogLine(
        """64.242.88.10 - - [07/Mar/2004:16:20:55 -0800] "GET /twiki/bin/view/Main/DCCAndPostFix HTTP/1.1" 200 5253""")
      .get
    apacheAccessLog.ipAddress should be ("64.242.88.10")
    apacheAccessLog.clientIdentd should be ("-")
    apacheAccessLog.userId should be ("-")
    apacheAccessLog.dateTime should be ("07/Mar/2004:16:20:55 -0800")
    apacheAccessLog.method should be ("GET")
    apacheAccessLog.endpoint should be ("/twiki/bin/view/Main/DCCAndPostFix")
    apacheAccessLog.protocol should be ("HTTP/1.1")
    apacheAccessLog.responseCode should be (200)
    apacheAccessLog.contentSize should be (5253)
  }

  it should "return empty value for a string with non-number response code" in {
    assert(
      ApacheAccessLog.parseLogLine(
        """64.242.88.10 - - [07/Mar/2004:16:20:55 -0800] "GET /twiki/bin/view/Main/DCCAndPostFix HTTP/1.1" 20x 5253""")
        .isEmpty)
  }

  it should "return empty value for a string with non-number content-size" in {
    assert(
      ApacheAccessLog.parseLogLine(
        """64.242.88.10 - - [07/Mar/2004:16:20:55 -0800] "GET /twiki/bin/view/Main/DCCAndPostFix HTTP/1.1" 200 52-3""")
        .isEmpty)
  }

  it should "return empty value for a string with incorrect timezone" in {
    assert(
      ApacheAccessLog.parseLogLine(
        """64.242.88.10 - - [07/Mar/2004:16:20:55 -080] "GET /twiki/bin/view/Main/DCCAndPostFix HTTP/1.1" 200 5253""")
        .isEmpty)
  }
}
