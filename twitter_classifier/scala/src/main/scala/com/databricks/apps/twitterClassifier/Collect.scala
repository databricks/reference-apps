package com.databricks.apps.twitterClassifier

import java.io.File
import com.google.gson.Gson
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** Collect at least the specified number of tweets into json text files. */
object Collect extends App {
  // Process program arguments and set properties
  if (args.length < 3) {
    System.err.println(s"Usage: ${ getClass.getSimpleName.replace("$", "") } <outputDirectory> <numTweetsToCollect> <intervalInSeconds> <partitionsEachInterval>")
    System.exit(-1)
  }

  val Array(outputDirectory, Utils.IntParam(numTweetsToCollect),  Utils.IntParam(intervalSecs), Utils.IntParam(partitionsEachInterval)) =
    Utils.parseOptions(args)
  val outputDir = new File(outputDirectory)
  if (outputDir.exists()) {
    System.err.println(s"ERROR - $outputDirectory already exists; please delete it or specify another directory")
    System.exit(-2)
  }
  outputDir.mkdirs()

  println("Initializing Streaming Spark Context...")
  val spark = SparkSession
    .builder()
    .appName(getClass.getSimpleName.replace("$", ""))
    //.config("spark.some.config.option", "some-value")
    .getOrCreate()

  val sc = spark.sparkContext
  val sqlContext = spark.sqlContext
  val ssc = new StreamingContext(sc, Seconds(intervalSecs))

  val gson = new Gson
  val tweetStream: DStream[String] = TwitterUtils.createStream(ssc, Utils.getAuth).map(gson.toJson(_))

  var numTweetsCollected = 0L
  tweetStream.foreachRDD { (rdd, time) =>
    val count = rdd.count
    if (count > 0) {
      val outputRDD = rdd.repartition(partitionsEachInterval)
      outputRDD.saveAsTextFile(s"$outputDirectory/tweets_${ time.milliseconds}")
      numTweetsCollected += count
      if (numTweetsCollected > numTweetsToCollect) System.exit(0)
    }
  }

  ssc.start()
  ssc.awaitTermination()
}
