package com.databricks.apps.logs;

import org.apache.commons.cli.*;

import org.apache.spark.streaming.Duration;

public class Flags {
  private static final Flags THE_INSTANCE = new Flags();

  private boolean isHelp;
  private Duration windowLength;
  private Duration slideInterval;
  private String logsDirectory;
  private String outputHtmlFile;
  private String checkpointDirectory;

  private boolean initialized = false;

  private Flags() {}

  public boolean isHelp() { return isHelp; }

  public Duration getWindowLength() {
    return windowLength;
  }

  public Duration getSlideInterval() {
    return slideInterval;
  }

  public String getLogsDirectory() {
    return logsDirectory;
  }

  public String getOutputHtmlFile() {
    return outputHtmlFile;
  }

  public String getCheckpointDirectory() {
    return checkpointDirectory;
  }

  public static Flags getInstance() {
    if (!THE_INSTANCE.initialized) {
      throw new RuntimeException("Flags have not been initialized");
    }
    return THE_INSTANCE;
  }

  public static void setFromCommandLineArgs(Options options, String[] args) {
    CommandLineParser parser = new PosixParser();
    try {
      CommandLine cl = parser.parse(options, args);
      THE_INSTANCE.isHelp = cl.hasOption(LogAnalyzerAppMain.HELP);
      THE_INSTANCE.windowLength = new Duration(Integer.parseInt(
          cl.getOptionValue(LogAnalyzerAppMain.WINDOW_LENGTH, "30")) * 1000);
      THE_INSTANCE.slideInterval = new Duration(Integer.parseInt(
          cl.getOptionValue(LogAnalyzerAppMain.SLIDE_INTERVAL, "5")) * 1000);
      THE_INSTANCE.logsDirectory = cl.getOptionValue(
          LogAnalyzerAppMain.LOGS_DIRECTORY, "/tmp/logs");
      THE_INSTANCE.outputHtmlFile = cl.getOptionValue(
          LogAnalyzerAppMain.OUTPUT_HTML_FILE, "/tmp/log_stats.html");
      THE_INSTANCE.checkpointDirectory = cl.getOptionValue(
          LogAnalyzerAppMain.CHECKPOINT_DIRECTORY, "/tmp/log-analyzer-streaming");
      THE_INSTANCE.initialized = true;
    } catch (ParseException e) {
      THE_INSTANCE.initialized = false;
      System.err.println("Parsing failed.  Reason: " + e.getMessage());
    }
  }
}
