package com.databricks.apps.twitterClassifier

import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** Pull live tweets and filters them for tweets in the chosen cluster */
object Predict extends App {
  if (args.length < 2) {
    System.err.println(s"Usage: ${ getClass.getSimpleName.replace("$", "") } <modelDirectory> <clusterNumber>")
    System.exit(-1)
  }

  val Array(modelFile, Utils.IntParam(clusterNumber)) = Utils.parseOptions(args)

  println("Initializing Streaming Spark Context...")
  val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
  val ssc = new StreamingContext(conf, Seconds(5))

  println("Initializing the the KMeans model...")
  val model: KMeansModel = new KMeansModel(ssc.sparkContext.objectFile[Vector](modelFile.toString).collect)

  println("Materializing Twitter stream...")
  TwitterUtils.createStream(ssc, Utils.getAuth)
    .map(_.getText)
    .filter(t => model.predict(Utils.featurize(t)) == clusterNumber)
    .foreachRDD(print _) // register DStream as an output stream and materialize it

  // Start the streaming computation
  println("Initialization complete.")
  ssc.start()
  ssc.awaitTermination()
}
