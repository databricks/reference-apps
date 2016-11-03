package com.databricks.apps.twitterClassifier

import com.google.gson.{Gson, GsonBuilder, JsonParser}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

/** Examine collected tweets and train a model based on them */
object ExamineAndTrain extends App {
  // Process program arguments and set properties
  if (args.length < 3) {
    System.err.println(s"Usage: ${ getClass.getSimpleName.replace("$", "") } <tweetInput> <outputModelDir> <numClusters> <numIterations>")
    System.exit(-2)
  }

  val Array(tweetInput, outputModelDir, Utils.IntParam(numClusters), Utils.IntParam(numIterations)) = args

  val spark = SparkSession
    .builder()
    .appName(getClass.getSimpleName.replace("$", ""))
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  val sc = spark.sparkContext
  val sqlContext = spark.sqlContext

  // Pretty print some of the tweets.
  val tweets = sc.textFile(tweetInput)
  println("------------Sample JSON Tweets-------")
  val gson: Gson = new GsonBuilder().setPrettyPrinting().create
  val jsonParser = new JsonParser
  tweets.take(5) foreach { tweet =>
    println(gson.toJson(jsonParser.parse(tweet)))
  }

  val tweetTable = sqlContext
    .read
    .json(tweetInput)
    .cache()
  tweetTable.registerTempTable("tweetTable")

  println("------Tweet table Schema---")
  tweetTable.printSchema()

  println("----Sample Tweet Text-----")
  sqlContext
    .sql("SELECT text FROM tweetTable LIMIT 10")
    .collect
    .foreach(println)

  println("------Sample Lang, Name, text---")
  sqlContext.sql("SELECT user.lang, user.name, text FROM tweetTable LIMIT 1000").collect.foreach(println)

  println("------Total count by languages Lang, count(*)---")
  sqlContext
    .sql("SELECT user.lang, COUNT(*) as cnt FROM tweetTable GROUP BY user.lang ORDER BY cnt DESC LIMIT 25")
    .collect
    .foreach(println)

  println("--- Training the model and persist it")
  val texts: Dataset[String] = sqlContext
    .sql("SELECT text from tweetTable")
    .map(_.toString)

  // Cache the vectors RDD since it will be used for all the KMeans iterations.
  val vectors = texts.rdd
    .map(Utils.featurize)
    .cache()

  vectors.count()  // Calls an action on the RDD to populate the vectors cache.
  val model: KMeansModel = KMeans.train(vectors, numClusters, numIterations)
  sc.makeRDD(model.clusterCenters, numClusters).saveAsObjectFile(outputModelDir)

  println("----100 example tweets from each cluster")
  0 until numClusters foreach { i =>
    println(s"\nCLUSTER $i:")
    texts.take(100) foreach { t =>
      if (model.predict(Utils.featurize(t)) == i) println(t)
    }
  }
}
