package com.databricks.apps.twitter_classifier

import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Pulls live tweets and filters them for tweets in the chosen cluster.
 */
object Predict {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: " + this.getClass.getSimpleName + " <modelDirectory>  <clusterNumber>")
      System.exit(1)
    }

    val Array(modelFile, Utils.IntParam(clusterNumber)) =
      Utils.parseCommandLineWithTwitterCredentials(args)

    println("Initializing Streaming Spark Context...")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(conf, Seconds(5))

    println("Initializing Twitter stream...")
    val tweets = TwitterUtils.createStream(ssc, Utils.getAuth)
    val statuses = tweets.map(_.getText)

    println("Initalizaing the the KMeans model...")
    val model = new KMeansModel(ssc.sparkContext.objectFile[Vector](modelFile.toString).collect())

    val filteredTweets = statuses
      .filter(t => model.predict(Utils.featurize(t)) == clusterNumber)
    filteredTweets.print()

    // Start the streaming computation
    println("Initialization complete.")
    ssc.start()
    ssc.awaitTermination()
  }
}
