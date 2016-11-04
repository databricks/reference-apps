package com.databricks.apps.twitterClassifier

import com.google.gson.{Gson, GsonBuilder, JsonParser}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.sql.{Dataset, SparkSession}

/** Examine collected tweets and train a model based on them */
object ExamineAndTrain extends App {
  val options = ExamineAndTrainOptions.parse(args)
  import options._

  if (!tweetDirectory.exists) {
    System.err.println(s"$tweetDirectory does not exist. Did you run Collect yet?")
    System.exit(-1)
  }
  if (!modelDirectory.exists) {
    System.err.println(s"${ modelDirectory.getCanonicalPath } does not exist. Did you run Collect yet?")
    System.exit(-2)
  }
  if (numClusters<1) {
    System.err.println(s"At least 1 clusters must be specified")
    System.exit(-3)
  }
  if (numIterations<1) {
    System.err.println(s"At least 1 iteration must be specified")
    System.exit(-4)
  }

  val spark = SparkSession
    .builder()
    .appName(getClass.getSimpleName.replace("$", ""))
    //.config("spark.some.config.option", "some-value")
    .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  val sc = spark.sparkContext
  val sqlContext = spark.sqlContext

  // Pretty print some of the tweets.
  val tweets = sc.textFile(tweetDirectory.getCanonicalPath)
  println("------------Sample JSON Tweets-------")
  val gson: Gson = new GsonBuilder().setPrettyPrinting().create
  val jsonParser = new JsonParser
  tweets.take(5) foreach { tweet =>
    println(gson.toJson(jsonParser.parse(tweet)))
  }

  val tweetTable = sqlContext
    .read
    .json(tweetDirectory.getCanonicalPath)
    .cache()
  tweetTable.createOrReplaceTempView("tweetTable")

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
  sc.makeRDD(model.clusterCenters, numClusters).saveAsObjectFile(modelDirectory.getCanonicalPath)

  println("----100 example tweets from each cluster")
  0 until numClusters foreach { i =>
    println(s"\nCLUSTER $i:")
    texts.take(100) foreach { t =>
      if (model.predict(Utils.featurize(t)) == i) println(t)
    }
  }
}
