package com.databricks.apps.logs

/** Container for the application options. */
sealed case class AppOptions(logsDirectory: String,
                             outputHtmlFile: String,
                             windowLength: Int,
                             slideInterval: Int,
                             checkpointDirectory: String)

object CmdlineArgumentsParser {

  /**
    * Parse command line arguments.
    * If unable to parse, then print help message and exit the application.
    *
    * @param args Array of command line arguments
    * @return Application options.
    */
  def parse(args: Array[String]): AppOptions = {
    import com.github.acrisci.commander.Program
    var program = new Program()
      .version("2.0")
      .option("-l, --logs-directory [path]", "Directory with input log files", required = true)
      .option("-o, --output-html-file [path]", "Output HTML file to write statistics", required = true)
      .option("-w, --window-length [number]", "Length of the aggregate window in seconds", required = true, fn = _.toInt)
      .option("-s, --slide-interval [number]", "Slide interval in seconds", required = true, fn = _.toInt)
      .option("-c, --checkpoint-directory [path]", "Directory for Spark checkpoints", required = true)
    if (args.isEmpty)
      program.help()
    program = program.parse(args)

    AppOptions(program.logsDirectory,
      program.outputHtmlFile,
      program.windowLength,
      program.slideInterval,
      program.checkpointDirectory)
  }
}