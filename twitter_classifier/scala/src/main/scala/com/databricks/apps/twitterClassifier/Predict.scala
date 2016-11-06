package com.databricks.apps.twitterClassifier

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Predict extends App {
  val options = PredictOptions.parse(args)
  new Predict(options).predict()
}

trait SparkSessionLike {
  def intervalInSecs: Int

  val spark = SparkSession
    .builder
    .appName(getClass.getSimpleName.replace("$", ""))
    .master("local[*]")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  // Suppress "WARN BlockManager: Block input-0-1478266015800 replicated to only 0 peer(s) instead of 1 peers" messages
  sc.setLogLevel("ERROR")
  val ssc = new StreamingContext(sc, Seconds(intervalInSecs))

  // Suppress "WARN BlockManager: Block input-0-1478266015800 replicated to only 0 peer(s) instead of 1 peers" messages
  ssc.sparkContext.setLogLevel("ERROR")
}

/** Pull live tweets and filter them for tweets in the chosen cluster */
class Predict(options: PredictOptions) extends SparkSessionLike {
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
