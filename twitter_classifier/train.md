# Train with Spark MLLib

This section covers how to train a language classifier using the text in the Tweets.

First, we need to featurize the Tweet text.  MLLib has a HashingTF class that does that:

```scala
object Utils {
  ...

  val numFeatures = 1000
  val tf = new HashingTF(numFeatures)

  /**
   * Create feature vectors by turning each tweet into bigrams of
   * characters (an n-gram model) and then hashing those to a
   * length-1000 feature vector that we can pass to MLlib.
   * This is a common way to decrease the number of features in a
   * model while still getting excellent accuracy. (Otherwise every
   * pair of Unicode characters would potentially be a feature.)
   */
  def featurize(s: String): Vector = {
    tf.transform(s.sliding(2).toSeq)
  }

  ...
}
```

This is the code that actually grabs the tweet text from the tweetTable and featurizes it.  K-Means is called to create the number of clusters and the algorithm is run for the specified number of iterations.  Finally, the trained model is persisted so it can be loaded later.
```scala
val texts = sqlContext.sql("SELECT text from tweetTable").map(_.head.toString)
// Caches the vectors since it will be used many times by KMeans.
val vectors = texts.map(Utils.featurize).cache()
vectors.count()  // Calls an action to create the cache.
val model = KMeans.train(vectors, numClusters, numIterations)
sc.makeRDD(model.clusterCenters, numClusters).saveAsObjectFile(outputModelDir)
```

Last, here is some code to take a sample set of tweets and print them out by cluster, so that we can see what language clusters our model contains.  Pick your favorite to use for part 3.
```scala
val some_tweets = texts.take(100)
for (i <- 0 until numClusters) {
  println(s"\nCLUSTER $i:")
  some_tweets.foreach { t =>
    if (model.predict(Utils.featurize(t)) == i) {
      println(t)
    }
  }
}
```
