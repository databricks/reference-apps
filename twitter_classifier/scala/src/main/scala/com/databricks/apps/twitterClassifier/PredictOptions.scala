package com.databricks.apps.twitterClassifier

import java.io.File
import com.github.acrisci.commander.Program

abstract sealed case class PredictOptions(
  twitterOptions: TwitterOptions,
  overWrite: Boolean = false,
  verbose: Boolean = false,
  modelDirectory: File = new java.io.File(System.getProperty("user.home"), "/sparkTwitter/model/"),
  intervalInSecs: Int = 5,
  clusterNumber: Int = 10
) extends Serializable

object PredictOptions extends TwitterOptionParser {
  override val _program: Program = super._program
    .option(flags="-v, --verbose",   description="Generate output to show progress")
    .option(flags="-x, --overWrite", description="Overwrite all data files from a previous run")
    .usage("Predict [options] <modelDirectory> <intervalInSeconds> <clusterNumber>")

  def parse(args: Array[String]): PredictOptions = {
    val program: Program = _program.parse(args)
    if (program.args.length!=program.usage.split(" ").length-2) program.help

    new PredictOptions(
      twitterOptions = super.apply(args),
      overWrite = program.overWrite,
      verbose = program.verbose,
      modelDirectory = new File(program.args.head.replaceAll("^~", System.getProperty("user.home"))),
      intervalInSecs = program.args(1).toInt,
      clusterNumber = program.args(2).toInt
    ){}
  }
}
