package com.databricks.apps.twitter_classifier

import com.google.gson.{GsonBuilder, JsonParser}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Examine the collected tweets and trains a model based on them.
 */
object ExamineAndTrain {
  val jsonParser = new JsonParser()
  val gson = new GsonBuilder().setPrettyPrinting().create()

  def main(args: Array[String]) {
    // Process program arguments and set properties
    if (args.length < 3) {
      System.err.println("Usage: " + this.getClass.getSimpleName +
        " <tweetInput> <outputModelDir> <numClusters> <numIterations>")
      System.exit(1)
    }
    val Array(tweetInput, outputModelDir, Utils.IntParam(numClusters), Utils.IntParam(numIterations)) = args

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Pretty print some of the tweets.
    val tweets = sc.textFile(tweetInput)
    println("------------Sample JSON Tweets-------")
    for (tweet <- tweets.take(5)) {
      println(gson.toJson(jsonParser.parse(tweet)))
    }

    val tweetTable = sqlContext.read.json(tweetInput).cache()
    tweetTable.registerTempTable("tweetTable")

    println("------Tweet table Schema---")
    tweetTable.printSchema()

    println("----Sample Tweet Text-----")
    sqlContext.sql("SELECT text FROM tweetTable LIMIT 10").collect().foreach(println)

    println("------Sample Lang, Name, text---")
    sqlContext.sql("SELECT user.lang, user.name, text FROM tweetTable LIMIT 1000").collect().foreach(println)

    println("------Total count by languages Lang, count(*)---")
    sqlContext.sql("SELECT user.lang, COUNT(*) as cnt FROM tweetTable GROUP BY user.lang ORDER BY cnt DESC LIMIT 25").collect.foreach(println)

    println("--- Training the model and persist it")
    val texts = sqlContext.sql("SELECT text from tweetTable").map(_.toString)
    // Cache the vectors RDD since it will be used for all the KMeans iterations.
    val vectors = texts.map(Utils.featurize).cache()
    vectors.count()  // Calls an action on the RDD to populate the vectors cache.
    val model = KMeans.train(vectors, numClusters, numIterations)
    sc.makeRDD(model.clusterCenters, numClusters).saveAsObjectFile(outputModelDir)

    val some_tweets = texts.take(100)
    println("----Example tweets from the clusters")
    for (i <- 0 until numClusters) {
      println(s"\nCLUSTER $i:")
      some_tweets.foreach { t =>
        if (model.predict(Utils.featurize(t)) == i) {
          println(t)
        }
      }
    }
  }
}
