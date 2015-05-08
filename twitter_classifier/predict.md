## Part 3: Apply the Model in Real Time

Spark Streaming is used to filter live tweets coming in, only accepting those that are classified as the specified cluster (type) of tweets.  It takes the following arguments:

1. modelDirectory - This the directory where the model that was trained in part 2 was persisted.
* clusterNumber - This is the cluster you want to select from part 2.  Only tweets that match this language cluster will be printed out.

This program is very simple - this is the bulk of the code below.  First, load up a Spark Streaming Context.  Second, create a Twitter DStream and map it to grab the text.  Third, load up the K-Means model that was trained in step 2.  Finally, apply the model on the tweets, filtering out only those that match the specified cluster, and print the matching tweets.

```scala
println("Initializing Streaming Spark Context...")
val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
val ssc = new StreamingContext(conf, Seconds(5))

println("Initializing Twitter stream...")
val tweets = TwitterUtils.createStream(ssc, Utils.getAuth)
val statuses = tweets.map(_.getText)

println("Initializing the KMeans model...")
val model = new KMeansModel(ssc.sparkContext.objectFile[Vector](
    modelFile.toString).collect())

val filteredTweets = statuses
  .filter(t => model.predict(Utils.featurize(t)) == clusterNumber)
filteredTweets.print()
```

Now, run [Predict.scala](scala/src/main/scala/com/databricks/apps/twitter_classifier/Predict.scala):

```bash
 %  ${YOUR_SPARK_HOME}/bin/spark-submit \
     --class "com.databricks.apps.twitter_classifier.Predict" \
     --master ${YOUR_SPARK_MASTER:-local[4]} \
     target/scala-2.10/spark-twitter-lang-classifier-assembly-1.0.jar \
     ${YOUR_MODEL_DIR:-/tmp/tweets/model} \
     ${CLUSTER_TO_FILTER:-7} \
     --consumerKey ${YOUR_TWITTER_CONSUMER_KEY} \
     --consumerSecret ${YOUR_TWITTER_CONSUMER_SECRET} \
     --accessToken ${YOUR_TWITTER_ACCESS_TOKEN}  \
     --accessTokenSecret ${YOUR_TWITTER_ACCESS_SECRET}
```
