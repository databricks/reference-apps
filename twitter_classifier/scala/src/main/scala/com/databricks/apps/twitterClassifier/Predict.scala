package com.databricks.apps.twitterClassifier

import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** Pull live tweets and filter them for tweets in the chosen cluster */
object Predict extends App {
  val options = PredictOptions.parse(args)
  import com.databricks.apps.twitterClassifier.SparkSessionThing._

  val ssc = new StreamingContext(sc, Seconds(options.intervalInSecs))

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
