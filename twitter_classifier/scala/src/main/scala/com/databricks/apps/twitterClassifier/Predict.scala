package com.databricks.apps.twitterClassifier

import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** Pull live tweets and filters them for tweets in the chosen cluster */
object Predict extends App {
  val options = PredictOptions.parse(args)
  import options._

  println("Initializing Streaming Spark Context...")
  val conf: SparkConf = new SparkConf().setAppName(getClass.getSimpleName.replace("$", ""))
  val ssc = new StreamingContext(conf, Seconds(5))

  // Suppress "WARN BlockManager: Block input-0-1478266015800 replicated to only 0 peer(s) instead of 1 peers" messages
  ssc.sparkContext.setLogLevel("ERROR")

  println("Initializing the the KMeans model...")
  val model: KMeansModel = new KMeansModel(ssc.sparkContext.objectFile[Vector](modelDirectory.getCanonicalPath).collect)

  println("Materializing Twitter stream...")
  TwitterUtils.createStream(ssc, Utils.maybeTwitterAuth)
    .map(_.getText)
    .filter(t => model.predict(Utils.featurize(t)) == clusterNumber)
    .foreachRDD(print _) // register DStream as an output stream and materialize it

  // Start the streaming computation
  println("Initialization complete.")
  ssc.start()
  ssc.awaitTermination()
}
