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

    val options = new ExamineAndTrainOptions(
      twitterOptions = super.apply(args),
      overWrite = program.overWrite,
      tweetDirectory = new File(program.args.head),
      modelDirectory = new File(program.args(1)),
      numClusters = program.args(2).toInt,
      numIterations = program.args(3).toInt
    ){}
    import options._

    if (!tweetDirectory.exists) {
      System.err.println(s"${ tweetDirectory.getCanonicalPath } does not exist. Did you run Collect yet?")
      System.exit(-1)
    }
    if (!modelDirectory.exists) {
      System.err.println(s"${ modelDirectory.getCanonicalPath } does not exist. Did you run Collect yet?")
      System.exit(-2)
    }
    if (numClusters<1) {
      System.err.println(s"At least 1 clusters must be specified")
      System.exit(-3)
    }
    if (numIterations<1) {
      System.err.println(s"At least 1 iteration must be specified")
      System.exit(-4)
    }

    options
  }
}
