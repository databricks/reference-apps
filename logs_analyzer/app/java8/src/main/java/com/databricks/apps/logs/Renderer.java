package com.databricks.apps.logs;

import java.io.*;
import java.util.List;
import java.util.Map;

import scala.Tuple2;
import scala.Tuple4;

public class Renderer implements Serializable {
  private final String fileTemplate;
  private final int windowLengthSec;

  public Renderer() throws IOException {
    this.windowLengthSec = (int) (Flags.getInstance().getWindowLength().milliseconds() / 1000);
    this.fileTemplate = TemplateProvider.fromResource("index.html.template");
  }

  public void render(LogStatistics allOfTime, LogStatistics lastWindow) throws IOException {
    // TODO: Replace this hacky String replace with a proper HTML templating library.
    String output = fileTemplate
            .replace("@{logLinesTable}", logLinesTable(allOfTime, lastWindow))
            .replace("@{contentSizesTable}", contentSizesTable(allOfTime, lastWindow))
            .replace("@{responseCodeTable}", responseCodeTable(allOfTime, lastWindow))
            .replace("@{topEndpointsTable}", topEndpointsTable(allOfTime, lastWindow))
            .replace("@{frequentIpAddressTable}", frequentIpAddressTable(allOfTime, lastWindow));
    writeOutput(output);
  }

  private String logLinesTable(LogStatistics allOfTime, LogStatistics lastWindow) {
    return "<table class=\"table table-striped\">" +
        String.format("<tr><th>All Of Time:</th><td>%s</td></tr>",
            allOfTime.getContentSizeStats()._1()) +
        String.format("<tr><th>Last %s seconds:</th><td>%s</td></tr>", windowLengthSec,
            lastWindow.getContentSizeStats()._1()) +
        "</table>";
  }

  private String contentSizesTable(LogStatistics allOfTime, LogStatistics lastWindow) {
    StringBuilder builder = new StringBuilder();
    builder.append("<table class=\"table table-striped\">");
    builder.append(String.format("<tr><th>Content Size</th><th>All of Time</th><th>Last %s seconds</th></tr>",
            windowLengthSec));
    Tuple4<Long, Long, Long, Long> totalStats = allOfTime.getContentSizeStats();
    Tuple4<Long, Long, Long, Long> lastStats = lastWindow.getContentSizeStats();
    builder.append(String.format("<tr><th>Avg:</th><td>%s</td><td>%s</td>",
        totalStats._1() > 0 ? totalStats._2() / totalStats._1() : "-",
        lastStats._1() > 0 ? lastStats._2() / lastStats._1() : "-"));
    builder.append(String.format("<tr><th>Min:</th><td>%s</td><td>%s</td>",
        totalStats._1() > 0 ? totalStats._3() : "-",
        lastStats._1() > 0 ? lastStats._3() : "-"));
    builder.append(String.format("<tr><th>Max:</th><td>%s</td><td>%s</td>",
        totalStats._1() > 0 ? totalStats._4() : "-",
        lastStats._1() > 0 ? lastStats._4() : "-"));
    builder.append("</table>");
    return builder.toString();
  }

  private String responseCodeTable(
      LogStatistics allOfTime, LogStatistics lastWindow) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("<table class=\"table table-striped\">");
    buffer.append(String.format("<tr><th>Response Code</th><th>All of Time</th><th>Last %s seconds</th></tr>",
            windowLengthSec));
    Map<Integer, Long> lastWindowMap = lastWindow.getResponseCodeToCount();
    for(Map.Entry<Integer, Long> entry: allOfTime.getResponseCodeToCount().entrySet()) {
      buffer.append(String.format("<tr><td>%s</td><td>%s</td><td>%s</td>",
        entry.getKey(), entry.getValue(), lastWindowMap.getOrDefault(entry.getKey(), 0L)));
    }
    buffer.append("</table>");
    return buffer.toString();
  }

  private String frequentIpAddressTable(
      LogStatistics allOfTime, LogStatistics lastWindow) {
    StringBuilder builder = new StringBuilder();
    builder.append("<table class=\"table table-striped\">");
    builder.append(String.format("<tr><th>All of Time</th><th>Last %s seconds</th></tr>",
            windowLengthSec));
    List<String> totalIpAddresses = allOfTime.getIpAddresses();
    List<String> windowIpAddresses = lastWindow.getIpAddresses();
    for (int i = 0; i < totalIpAddresses.size(); i++) {
      builder.append(String.format("<tr><td>%s</td><td>%s</td></tr>",
          totalIpAddresses.get(i),
          i < windowIpAddresses.size() ? windowIpAddresses.get(i) : "-"));
    }
    builder.append("</table>");
    return builder.toString();
  }

  private String topEndpointsTable(
      LogStatistics allOfTime, LogStatistics lastWindow) {
    StringBuilder builder = new StringBuilder();
    builder.append("<table class=\"table table-striped\">");
    builder.append(String.format("<tr><th>All of Time</th><th>Last %s seconds</th></tr>",
            windowLengthSec));
    List<Tuple2<String, Long>> totalTopEndpoints = allOfTime.getTopEndpoints();
    List<Tuple2<String, Long>> windowTopEndpoints = lastWindow.getTopEndpoints();
    for (int i = 0; i < totalTopEndpoints.size(); i++) {
      builder.append(String.format("<tr><td>%s</td><td>%s</td></tr>",
          totalTopEndpoints.get(i)._1(),
          i < windowTopEndpoints.size() ? windowTopEndpoints.get(i)._1() : "-"));
    }
    builder.append("</table>");
    return builder.toString();
  }

  private void writeOutput(String output) throws IOException {
    try (Writer out = new BufferedWriter(new FileWriter(Flags.getInstance().getOutputHtmlFile()))) {
      out.write(output);
    }
  }
}
