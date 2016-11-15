package com.databricks.apps.twitterClassifier

import com.github.acrisci.commander.Program
import java.io.File

abstract sealed case class CollectOptions(
  twitterOptions: TwitterOptions,
  overWrite: Boolean = false,
  tweetDirectory: File = new File(System.getProperty("user.home"), "/sparkTwitter/tweets/"),
  numTweetsToCollect: Int = 100,
  intervalInSecs: Int = 1,
  partitionsEachInterval: Int = 1
)

object CollectOptions extends TwitterOptionParser {
  override val _program = super._program
    .option(flags="-w, --overWrite", description="Overwrite all data files from a previous run")
    .usage("Collect [options] <tweetDirectory> <numTweetsToCollect> <intervalInSeconds> <partitionsEachInterval>")

  def parse(args: Array[String]): CollectOptions = {
    val program: Program = _program.parse(args)
    if (program.args.length!=program.usage.split(" ").length-2) program.help

    new CollectOptions(
      twitterOptions = super.apply(args),
      overWrite = program.overWrite,
      tweetDirectory = new File(program.args.head.replaceAll("^~", System.getProperty("user.home"))),
      numTweetsToCollect = program.args(1).toInt,
      intervalInSecs = program.args(2).toInt,
      partitionsEachInterval = program.args(3).toInt
    ){}
  }
}
