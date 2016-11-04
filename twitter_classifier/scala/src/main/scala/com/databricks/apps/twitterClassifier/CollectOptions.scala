package com.databricks.apps.twitterClassifier

import java.io.File
import com.github.acrisci.commander.Program

abstract sealed case class CollectOptions(
  twitterOptions: TwitterOptions,
  overWrite: Boolean = false,
  tweetDirectory: File = new File("~/twitterClassifier/tweets/"),
  numTweetsToCollect: Int = 100,
  intervalInSecs: Int = 1,
  partitionsEachInterval: Int = 1
)

object CollectOptions extends CommonOptions {
  override val _program = super._program
    .option(flags="-x, --overWrite", description="Overwrite data files on each run [false]")
    .epilogue("<tweetDirectory> <numTweetsToCollect> <intervalInSeconds> <partitionsEachInterval>")

  def parse(args: Array[String]): CollectOptions = {
    val program: Program = _program.parse(args)
    if (program.args.length!=program.epilogue.split(" ").length) program.help

    val x: Boolean = program.overWrite
    new CollectOptions(
      twitterOptions = super.apply(args),
      overWrite = program.overWrite,
      tweetDirectory = new File(program.args.head),
      numTweetsToCollect = program.args(1).toInt,
      intervalInSecs = program.args(2).toInt,
      partitionsEachInterval = program.args(3).toInt
    ){}
  }
}
