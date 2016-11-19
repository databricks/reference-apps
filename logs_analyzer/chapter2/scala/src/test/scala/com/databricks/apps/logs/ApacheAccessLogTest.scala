package com.databricks.apps.logs

import org.scalatest._

class ApacheAccessLogTest extends FlatSpec {

  "parseLogLine" should "throw IOException for null argument" in {
    assertThrows[RuntimeException] {
      ApacheAccessLog.parseLogLine(null)
    }
  }

  it should "throw IOException for empty string argument" in {
    assertThrows[RuntimeException] {
      ApacheAccessLog.parseLogLine("")
    }
  }

  it should "correctly parse all log entry parameters" in {
    val apacheAccessLog = ApacheAccessLog.parseLogLine(
      """64.242.88.10 - - [07/Mar/2004:16:20:55 -0800] "GET /twiki/bin/view/Main/DCCAndPostFix HTTP/1.1" 200 5253""")
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

  it should "throw IOException for a string with non-number response code" in {
    assertThrows[RuntimeException] {
      ApacheAccessLog.parseLogLine(
        """64.242.88.10 - - [07/Mar/2004:16:20:55 -0800] "GET /twiki/bin/view/Main/DCCAndPostFix HTTP/1.1" 20x 5253""")
    }
  }

  it should "throw IOException for a string with non-number content-size" in {
    assertThrows[RuntimeException] {
      ApacheAccessLog.parseLogLine(
        """64.242.88.10 - - [07/Mar/2004:16:20:55 -0800] "GET /twiki/bin/view/Main/DCCAndPostFix HTTP/1.1" 200 52-3""")
    }
  }

  it should "throw IOException for a string with incorrect timezone" in {
    assertThrows[RuntimeException] {
      ApacheAccessLog.parseLogLine(
        """64.242.88.10 - - [07/Mar/2004:16:20:55 -080] "GET /twiki/bin/view/Main/DCCAndPostFix HTTP/1.1" 200 5253""")
    }
  }
}