package com.databricks.apps.twitterClassifier

import java.io.File
import com.github.acrisci.commander.Program

abstract sealed case class PredictOptions(
  twitterOptions: TwitterOptions,
  overWrite: Boolean = false,
  modelDirectory: File = new File("~/sparkTwitter/model/"),
  clusterNumber: Int = 10
)

object PredictOptions extends TwitterOptionParser {
  override val _program: Program = super._program
    .option(flags="-x, --overWrite", description="Overwrite all data files from a previous run [false]", default=false)
    .usage("Predict [options] <modelDirectory> <clusterNumber>")

  def parse(args: Array[String]): PredictOptions = {
    val program: Program = _program.parse(args)
    if (program.args.length!=program.usage.split(" ").length-2) program.help

    new PredictOptions(
      twitterOptions = super.apply(args),
      overWrite = program.overWrite,
      modelDirectory = new File(program.args.head),
      clusterNumber = program.args(1).toInt
    ){}
  }
}
