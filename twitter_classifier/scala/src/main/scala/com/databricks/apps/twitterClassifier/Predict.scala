package com.databricks.apps.twitterClassifier

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Predict extends App {
  import SparkSetup._

  val options = PredictOptions.parse(args)
  val ssc = new StreamingContext(sc, Seconds(options.intervalInSecs))
  Predictor.doIt(options, sc, ssc)
}

/** Pull live tweets and filter them for tweets in the chosen cluster */
object Predictor {
  def doIt(options: PredictOptions, sc: SparkContext, ssc: StreamingContext) {
    println("Initializing the the KMeans model...")
    val model: KMeansModel = new KMeansModel(sc.objectFile[Vector](options.modelDirectory.getCanonicalPath).collect)

    println("Materializing Twitter stream...")
    TwitterUtils.createStream(ssc, maybeTwitterAuth)
      .map(_.getText)
      .foreachRDD { rdd =>
        rdd.filter(t => model.predict(featurize(t)) == options.clusterNumber)
           .foreach(print)  // register DStream as an output stream and materialize it
      }
    println("Initialization complete, starting streaming computation.")
    ssc.start()
    ssc.awaitTermination()
  }
}
