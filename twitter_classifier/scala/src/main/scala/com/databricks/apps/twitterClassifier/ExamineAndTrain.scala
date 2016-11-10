package com.databricks.apps.twitterClassifier

import com.google.gson.{Gson, GsonBuilder, JsonParser}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

/** Examine collected tweets and train a model based on them */
object ExamineAndTrain extends App {
  val options = ExamineAndTrainOptions.parse(args)

  new ExamineAndTrain(options)
    .examineAndTrain()
}

class ExamineAndTrain(options: ExamineAndTrainOptions) {
  import options._
  import SparkSetup._

  def examineAndTrain(): Unit = {
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    if (verbose) { // Pretty-print some of the tweets
      val tweets: RDD[String] = sc.textFile(tweetDirectory.getCanonicalPath)
      println("------------Sample JSON Tweets-------")
      val gson: Gson = new GsonBuilder().setPrettyPrinting().create
      val jsonParser = new JsonParser
      tweets.take(5) foreach { tweet =>
        println(gson.toJson(jsonParser.parse(tweet)))
      }
    }

    val tweetTable = sqlContext
      .read
      .json(tweetDirectory.getCanonicalPath)
      .cache()
    tweetTable.createOrReplaceTempView("tweetTable")

    if (verbose) {
      println("------Tweet table Schema---")
      tweetTable.printSchema()
      println("----Sample Tweet Text-----")

      sqlContext
        .sql("SELECT text FROM tweetTable LIMIT 10")
        .collect
        .foreach(println)

      println("------Sample Lang, Name, text---")
      sqlContext
        .sql("SELECT user.lang, user.name, text FROM tweetTable LIMIT 1000")
        .collect
        .foreach(println)

      println("------Total count by languages Lang, count(*)---")
      sqlContext
        .sql("SELECT user.lang, COUNT(*) as cnt FROM tweetTable GROUP BY user.lang ORDER BY cnt DESC LIMIT 25")
        .collect
        .foreach(println)

      println("--- Training the model and persisting it")
    }
    val texts: Dataset[String] = sqlContext
      .sql("SELECT text from tweetTable")
      .map(_.toString)

    // Cache the vectors RDD since it will be used for all the KMeans iterations.
    val vectors = texts.rdd
      .map(featurize)
      .cache()

    vectors.count()  // Calls an action on the RDD to populate the vectors cache.
    val model: KMeansModel = KMeans.train(vectors, numClusters, numIterations)
    sc.makeRDD(model.clusterCenters, numClusters)
      .saveAsObjectFile(modelDirectory.getCanonicalPath)

    if (verbose) {
      println("----100 example tweets from each cluster")
      0 until numClusters foreach { i =>
        println(s"\nCLUSTER $i:")
        texts.take(100) foreach { t =>
          if (model.predict(featurize(t)) == i) println(t)
        }
      }
    }
  }
}
