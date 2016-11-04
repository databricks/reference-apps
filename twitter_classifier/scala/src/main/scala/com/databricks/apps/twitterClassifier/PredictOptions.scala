package com.databricks.apps.twitterClassifier

import java.io.File
import com.github.acrisci.commander.Program

abstract sealed case class PredictOptions(
  twitterOptions: TwitterOptions,
  overWrite: Boolean = false,
  modelDirectory: File = new File("~/twitterClassifier/model/"),
  clusterNumber: Int = 10
)

object PredictOptions extends CommonOptions {
  override val _program = super._program
    .option(flags="-x, --overWrite", description="Overwrite data files on each run [false]", default=false)
    .epilogue("<modelDirectory> <clusterNumber>")

  def parse(args: Array[String]): PredictOptions = {
    val program: Program = _program.parse(args)
    if (program.args.length!=program.epilogue.split(" ").length) program.help

    new PredictOptions(
      twitterOptions = super.apply(args),
      overWrite = program.overWrite,
      modelDirectory = new File(program.args.head),
      clusterNumber = program.args(1).toInt
    ){}
  }
}
