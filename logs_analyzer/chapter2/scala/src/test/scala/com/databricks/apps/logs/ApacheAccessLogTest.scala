package com.databricks.apps.logs

import org.scalatest._

class ApacheAccessLogTest extends FlatSpec with Matchers {

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
    val apacheAccessLog = ApacheAccessLog
      .parseLogLine(
        """64.242.88.10 - - [07/Mar/2004:16:20:55 -0800] "GET /twiki/bin/view/Main/DCCAndPostFix HTTP/1.1" 200 5253""")
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