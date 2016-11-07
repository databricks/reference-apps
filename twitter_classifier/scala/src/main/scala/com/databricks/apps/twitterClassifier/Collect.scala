package com.databricks.apps.twitterClassifier

import com.google.gson.Gson
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils

/** Collect at least the specified number of tweets into json text files. */
object Collect extends App {
  val options = CollectOptions.parse(args)
  import SparkSessionThing._

  val ssc = new StreamingContext(sc, Seconds(options.intervalInSecs))
  val tweetStream: DStream[String] = TwitterUtils.createStream(ssc, maybeTwitterAuth)
    .map(new Gson().toJson(_))

  var numTweetsCollected = 0L
  tweetStream.foreachRDD { (rdd, time) =>
    val count = rdd.count
    if (count > 0) {
      rdd.saveAsTextFile(options.tweetDirectory.getAbsolutePath)
      numTweetsCollected += count
      if (numTweetsCollected > options.numTweetsToCollect) System.exit(0)
    }
  }

  ssc.start()
  ssc.awaitTermination()
}
