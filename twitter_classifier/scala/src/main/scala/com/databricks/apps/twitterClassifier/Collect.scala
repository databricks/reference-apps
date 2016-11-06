package com.databricks.apps.twitterClassifier

import com.google.gson.Gson
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils

/** Collect at least the specified number of tweets into json text files. */
object Collect extends App {
  val options = CollectOptions.parse(args)
  import com.databricks.apps.twitterClassifier.Collect.options._
  val intervalInSecs = options.intervalInSecs

  if (tweetDirectory.exists) {
    if (options.overWrite) { // nuke any existing files
      options.tweetDirectory.listFiles.foreach(_.delete)
    } else {
      System.err.println(s"FATAL: $tweetDirectory already exists; please delete it, specify --overWrite, or specify another directory")
      System.exit(-2)
    }
  } else
    tweetDirectory.mkdirs()
  new Collect(options)
}

class Collect(options: CollectOptions) extends StreamingSessionLike {
  val tweetStream: DStream[String] = TwitterUtils.createStream(ssc, Utils.maybeTwitterAuth)
    .map(new Gson().toJson(_))

  var numTweetsCollected = 0L
  tweetStream.foreachRDD { (rdd, time) =>
    val count = rdd.count
    if (count > 0) {
      rdd.saveAsTextFile(tweetDirectory.getAbsolutePath)
      numTweetsCollected += count
      if (numTweetsCollected > numTweetsToCollect) System.exit(0)
    }
  }

  ssc.start()
  ssc.awaitTermination()
}
