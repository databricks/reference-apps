package com.databricks.apps.logs

import org.scalatest.FlatSpec

class ApacheAccessLogTest extends FlatSpec {

  "parseLogLine" should "return empty value for null argument" in {
    assert(ApacheAccessLog.parseLogLine(null).isEmpty)
  }

  it should "return empty value for empty string argument" in {
    assert(ApacheAccessLog.parseLogLine("").isEmpty)
  }

  it should "correctly parse all log entry parameters" in {
    val apacheAccessLog = ApacheAccessLog.parseLogLine(
      """64.242.88.10 - - [07/Mar/2004:16:20:55 -0800] "GET /twiki/bin/view/Main/DCCAndPostFix HTTP/1.1" 200 5253""").get
    assert(apacheAccessLog.ipAddress == "64.242.88.10")
    assert(apacheAccessLog.clientIdentd == "-")
    assert(apacheAccessLog.userId == "-")
    assert(apacheAccessLog.dateTime == "07/Mar/2004:16:20:55 -0800")
    assert(apacheAccessLog.method == "GET")
    assert(apacheAccessLog.endpoint == "/twiki/bin/view/Main/DCCAndPostFix")
    assert(apacheAccessLog.protocol == "HTTP/1.1")
    assert(apacheAccessLog.responseCode == 200)
    assert(apacheAccessLog.contentSize == 5253)

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
