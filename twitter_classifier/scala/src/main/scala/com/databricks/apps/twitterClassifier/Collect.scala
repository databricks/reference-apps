package com.databricks.apps.twitterClassifier

import com.google.gson.Gson
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** Collect at least the specified number of tweets into json text files. */
object Collect extends App {
  val options = CollectOptions.parse(args)
  import options._

  if (tweetDirectory.exists) {
    if (options.overWrite) { // nuke any existing files
      options.tweetDirectory.listFiles.foreach(_.delete)
    } else {
      System.err.println(s"FATAL: $tweetDirectory already exists; please delete it, specify --overWrite, or specify another directory")
      System.exit(-2)
    }
  } else
    tweetDirectory.mkdirs()

  println("Initializing Streaming Spark Context...")
  val spark = SparkSession
    .builder
    .appName(getClass.getSimpleName.replace("$", ""))
    //.config("spark.some.config.option", "some-value")
    .master("local")
    .getOrCreate()

  val sc = spark.sparkContext
  val sqlContext = spark.sqlContext
  val ssc = new StreamingContext(sc, Seconds(intervalInSecs))

  val gson = new Gson
  val tweetStream: DStream[String] = TwitterUtils.createStream(ssc, Utils.maybeTwitterAuth)
    .map(gson.toJson(_))

  var numTweetsCollected = 0L
  tweetStream.foreachRDD { (rdd, time) =>
    val count = rdd.count
    if (count > 0) {
      val outputRDD = rdd.repartition(partitionsEachInterval)
      outputRDD.saveAsTextFile(s"$tweetDirectory/tweets_${ time.milliseconds}")
      numTweetsCollected += count
      if (numTweetsCollected > numTweetsToCollect) System.exit(0)
    }
  }

  ssc.start()
  ssc.awaitTermination()
}
