package com.databricks.apps.twitterClassifier

import java.io.File
import com.github.acrisci.commander.Program

abstract sealed case class ExamineAndTrainOptions(
  twitterOptions: TwitterOptions,
  overWrite: Boolean = false,
  tweetDirectory: File = new File("~/twitterClassifier/tweets/"),
  modelDirectory: File = new File("~/twitterClassifier/modelDirectory/"),
  numClusters: Int = 10,
  numIterations: Int = 100
)

object ExamineAndTrainOptions extends CommonOptions {
  override val _program = super._program
    .option(flags="-x, --overWrite", description="Overwrite data files on each run [false]", default=false)
    .epilogue("<tweetDirectory> <modelDirectory> <numClusters> <numIterations>")

  def parse(args: Array[String]): ExamineAndTrainOptions = {
    val program: Program = _program.parse(args)
    if (program.args.length!=program.epilogue.split(" ").length) program.help

    new ExamineAndTrainOptions(
      twitterOptions = super.apply(args),
      overWrite = program.overWrite,
      new File(program.args.head),
      new File(program.args(1)),
      program.args(2).toInt,
      program.args(3).toInt
    ){}
  }
}
