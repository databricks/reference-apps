package com.databricks.apps.twitter_classifier

import java.io.{File, PrintWriter}

import com.google.gson.Gson
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Collect at least the specified number of tweets into json text files.
 */
object Collect {
  private var numTweetsCollected = 0L
  private var partNum = 0
  private var gson = new Gson()

  def main(args: Array[String]) {
    // Process program arguments and set properties
    if (args.length < 3) {
      System.err.println("Usage: " + this.getClass.getSimpleName +
        "<outputDirectory> <intervalInSeconds> <numTweetsToCollect>")
      System.exit(1)
    }
    val Array(outputDirectory, Utils.IntParam(intervalSecs), Utils.IntParam(numTweetsToCollect)) =
      Utils.parseCommandLineWithTwitterCredentials(args)
    val outputDir = new File(outputDirectory.toString)
    if (outputDir.exists()) {
      System.err.println("ERROR - %s already exists: delete or specify another directory".format(
        outputDirectory))
      System.exit(1)
    }
    outputDir.mkdirs()

    println("Initializing Streaming Spark Context...")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(intervalSecs))
    @transient val sqlContext = new SQLContext(sc)

    val tweetStream = TwitterUtils.createStream(ssc, Utils.getAuth)

    tweetStream.foreachRDD(rdd => {
      val count = rdd.count()
      if (count > 0) {
        val writer = new PrintWriter("%s/part-%05d".format(outputDirectory, partNum), "UTF-8")
        val tweetJsonArray = rdd.collect()
        for (tweetJson <- tweetJsonArray) {
            try {
              val jsonString = gson.toJson(tweetJson)

              // TODO(vida): Remove this workaround when SPARK-3390 is fixed.
              val rdd = sc.parallelize(jsonString :: Nil)
              sqlContext.jsonRDD(rdd).count()

              writer.println(jsonString)
              numTweetsCollected = numTweetsCollected + 1
            } catch {
              case e: Exception =>
                println("***** Triggered SPARK-3390, ignoring.")
            }
          }
        writer.close()

        partNum = partNum + 1
        println("The current count of tweets is: %s".format(numTweetsCollected))
        if (numTweetsCollected > numTweetsToCollect) {
          println("Collected the necessary number of tweets, exiting")
          System.exit(0)
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
