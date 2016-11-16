package com.databricks.apps.twitterClassifier

import com.github.acrisci.commander.Program

case class TwitterOptions(
  consumerKey: String,
  consumerSecret: String,
  accessToken: String,
  accessTokenSecret: String
) extends Serializable

trait TwitterOptionParser {
  def _program = new Program()
    .version("2.0.0")
    .option(flags="-c, --consumerKey [type]",       description="Twitter OAuth Consumer Key",        required=true)
    .option(flags="-s, --consumerSecret [type]",    description="Twitter OAuth Consumer Secret",     required=true)
    .option(flags="-t, --accessToken [type]",       description="Twitter OAuth Access Token",        required=true)
    .option(flags="-v, --accessTokenSecret [type]", description="Twitter OAuth Access Token Secret", required=true)

  def apply(args: Array[String]): TwitterOptions = {
    val twitterOptions = TwitterOptions(
      consumerKey = _program.consumerKey,
      consumerSecret = _program.consumerSecret,
      accessToken = _program.accessToken,
      accessTokenSecret = _program.accessTokenSecret
    )
    System.setProperty("twitter4j.oauth.consumerKey",       twitterOptions.consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret",    twitterOptions.consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken",       twitterOptions.accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", twitterOptions.accessTokenSecret)

    twitterOptions
  }
}
