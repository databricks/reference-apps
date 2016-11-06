package com.databricks.apps.twitterClassifier

import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.streaming.twitter._

object Predict extends App {
  val options = PredictOptions.parse(args)
  new Predict(options).predict()
}

/** Pull live tweets and filter them for tweets in the chosen cluster */
class Predict(options: PredictOptions) extends StreamingSessionLike {
  import options._
  val intervalInSecs = options.intervalInSecs

  def predict(): Unit = {
    println("Initializing the the KMeans model...")
    val model: KMeansModel = new KMeansModel(sc.objectFile[Vector](modelDirectory.getCanonicalPath).collect)

    println("Materializing Twitter stream...")
    TwitterUtils.createStream(ssc, Utils.maybeTwitterAuth)
      .map(_.getText)
      .foreachRDD { rdd =>
        rdd.filter(t => model.predict(Utils.featurize(t)) == clusterNumber)
           .foreach(print)  // register DStream as an output stream and materialize it
      }
    println("Initialization complete, starting streaming computation.")
    ssc.start()
    ssc.awaitTermination()
  }
}
