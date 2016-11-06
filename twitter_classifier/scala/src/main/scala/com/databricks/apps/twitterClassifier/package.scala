package com.databricks.apps.twitterClassifier

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

trait SparkSessionLike {
  println("Initializing Streaming Spark Context...")
  val spark = SparkSession
    .builder
    .appName(getClass.getSimpleName.replace("$", ""))
    .master("local[*]")
    .getOrCreate()

  val sqlContext = spark.sqlContext

  val sc: SparkContext = spark.sparkContext
  // Suppress "WARN BlockManager: Block input-0-1478266015800 replicated to only 0 peer(s) instead of 1 peers" messages
  sc.setLogLevel("ERROR")
}

trait StreamingSessionLike extends SparkSessionLike {
  def intervalInSecs: Int

  val ssc = new StreamingContext(sc, Seconds(intervalInSecs))
}
