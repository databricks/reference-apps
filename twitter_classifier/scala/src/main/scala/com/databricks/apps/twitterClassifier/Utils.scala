package com.databricks.apps.twitterClassifier

import org.apache.commons.cli.{CommandLine, DefaultParser, Options, ParseException, PosixParser}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.feature.HashingTF
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object Utils {
  val numFeatures = 1000
  val tf = new HashingTF(numFeatures)

  val CONSUMER_KEY = "consumerKey"
  val CONSUMER_SECRET = "consumerSecret"
  val ACCESS_TOKEN = "accessToken"
  val ACCESS_TOKEN_SECRET = "accessTokenSecret"

  val options: Options = new Options()
    .addOption(CONSUMER_KEY,        true, "Twitter OAuth Consumer Key")
    .addOption(CONSUMER_SECRET,     true, "Twitter OAuth Consumer Secret")
    .addOption(ACCESS_TOKEN,        true, "Twitter OAuth Access Token")
    .addOption(ACCESS_TOKEN_SECRET, true, "Twitter OAuth Access Token Secret")

  def parseOptions(args: Array[String]): Array[String] = {
    import scala.collection.JavaConverters._
    try {
      val cl: CommandLine = new DefaultParser().parse(options, args)
      options.getOptions.asScala.foreach { option =>
        if (Option(option.getValues).isEmpty) {
          System.err.println(s"FATAL: ${ option.getDescription } (--${ option.getOpt } option) was not specified.")
          System.exit(2)
        }
      }
      System.setProperty("twitter4j.oauth.consumerKey",       cl.getOptionValue(CONSUMER_KEY))
      System.setProperty("twitter4j.oauth.consumerSecret",    cl.getOptionValue(CONSUMER_SECRET))
      System.setProperty("twitter4j.oauth.accessToken",       cl.getOptionValue(ACCESS_TOKEN))
      System.setProperty("twitter4j.oauth.accessTokenSecret", cl.getOptionValue(ACCESS_TOKEN_SECRET))
      cl.getArgList.toArray(new Array[String](options.getOptions.size))
    } catch {
      case e: Exception =>
        System.err.println(s"Parsing failed. ${ e.getMessage }")
        sys.exit(-1)
    }
  }

  def getAuth: Some[OAuthAuthorization] = Some(new OAuthAuthorization(new ConfigurationBuilder().build))

  /** Create feature vectors by turning each tweet into bigrams of characters (an n-gram model)
   * and then hashing those to a length-1000 feature vector that we can pass to MLlib.
   * This is a common way to decrease the number of features in a model while still
   * getting excellent accuracy (otherwise every pair of Unicode characters would
   * potentially be a feature). */
  def featurize(s: String): Vector = tf.transform(s.sliding(2).toSeq)

  object IntParam {
    def unapply(str: String): Option[Int] = {
      try {
        Some(str.toInt)
      } catch {
        case e: NumberFormatException => None
      }
    }
  }
}
