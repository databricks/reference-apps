package com.databricks.apps.logs

import java.io.{BufferedWriter, FileWriter}

/** Renders statistics in HTML file. */
class Renderer(outputHtmlFile: String, windowLengthSec: Long) {
  private val template = TemplateProvider.fromResource("index.html.template")

  def render(allOfTime: LogStatistics, lastWindow: LogStatistics): Unit = {
    // TODO: Replace this hacky String replace with a proper HTML templating library.
    val output = template
      .replace("@{logLinesTable}", logLinesTable(allOfTime, lastWindow))
      .replace("@{contentSizesTable}", contentSizesTable(allOfTime, lastWindow))
      .replace("@{responseCodeTable}", responseCodeTable(allOfTime, lastWindow))
      .replace("@{topEndpointsTable}", topEndpointsTable(allOfTime, lastWindow))
      .replace("@{frequentIpAddressTable}", frequentIpAddressTable(allOfTime, lastWindow))
    writeOutput(output)
  }

  private def logLinesTable(allOfTime: LogStatistics, lastWindow: LogStatistics): String =
    ("""<table class="table table-striped">"""
      + s"<tr><th>All Of Time:</th><td>${allOfTime.contentSizeStats._1}</td></tr>"
      + s"<tr><th>Last $windowLengthSec seconds:</th><td>${lastWindow.contentSizeStats._1}</td></tr>"
      + "</table>")

  private def contentSizesTable(allOfTime: LogStatistics, lastWindow: LogStatistics): String = {
    val totalStats = allOfTime.contentSizeStats
    val lastStats = lastWindow.contentSizeStats
    ("""<table class="table table-striped">"""
      + s"<tr><th>Content Size</th><th>All of Time</th><th>Last $windowLengthSec seconds</th></tr>"
      + s"<tr><th>Avg:</th><td>${if (totalStats._1 > 0) totalStats._2 / totalStats._1 else "-"}</td>"
      + s"<td>${if (lastStats._1 > 0) lastStats._2 / lastStats._1 else "-"}</td>"
      + s"<tr><th>Min:</th><td>${if (totalStats._1 > 0) totalStats._3 else "-"}</td>"
      + s"<td>${if (lastStats._1 > 0) lastStats._3 else "-"}</td>"
      + s"<tr><th>Max:</th><td>${if (totalStats._1 > 0) totalStats._4 else "-"}</td>"
      + s"<td>${if (lastStats._1 > 0) lastStats._4 else "-"}</td>"
      + "</table>")
  }

  private def responseCodeTable(allOfTime: LogStatistics, lastWindow: LogStatistics): String = {
    val lastWindowResponseCodes = lastWindow.responseCodeToCount
    ("""<table class="table table-striped">"""
      + s"<tr><th>Response Code</th><th>All of Time</th><th>Last $windowLengthSec seconds</th></tr>"
      + allOfTime.responseCodeToCount
      .map(e => s"<tr><td>${e._1}</td><td>${e._2}</td><td>${lastWindowResponseCodes.getOrElse(e._1, "-")}</td>")
      .mkString
      + "</table>")
  }

  private def frequentIpAddressTable(allOfTime: LogStatistics, lastWindow: LogStatistics): String =
    ("""<table class="table table-striped">"""
      + s"<tr><th>All of Time</th><th>Last $windowLengthSec seconds</th></tr>"
      + allOfTime.ipAddresses
      .zipAll(lastWindow.ipAddresses, "-", "-")
      .map(e => s"<tr><td>${e._1}</td><td>${e._2}</td></tr>").mkString
      + "</table>")

  private def topEndpointsTable(allOfTime: LogStatistics, lastWindow: LogStatistics): String =
    ("""<table class="table table-striped">"""
      + s"<tr><th>All of Time</th><th>Last $windowLengthSec seconds</th></tr>"
      + allOfTime.topEndpoints.keys
      .zipAll(lastWindow.topEndpoints.keys, "-", "-")
      .map(e => s"<tr><td>${e._1}</td><td>${e._2}</td></tr>").mkString
      + "</table>")

  private def writeOutput(output: String): Unit = {
    import resource._
    for (out <- managed(new BufferedWriter(new FileWriter(outputHtmlFile)))) {
      out.write(output)
    }
  }
}
