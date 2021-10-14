# Part 1: Collect a Dataset of Tweets

Spark Streaming is used to collect tweets as the dataset.  The tweets are written out in JSON format, one tweet per line.  A file of tweets is written every time interval until at least the desired number of tweets is collected.

See [Collect.scala](scala/src/main/scala/com/databricks/apps/twitter_classifier/Collect.scala) for the full code.  We'll walk through some
of the interesting bits now.

Collect.scala takes in the following argument list:

1. *outputDirectory* - the output directory for writing the tweets.  The files will be named 'part-%05d'
*  *numTweetsToCollect* - this is the minimum number of tweets to collect before the program exits.
*  *intervalInSeconds* - write out a new set of tweets every interval.
*  *partitionsEachInterval* - this is used to control the number of output files written for each interval

Collect.scala will also require [Twitter API Credentials](https://apps.twitter.com/). The Twitter credentials are passed in through command line flags.

Below is a snippet of the actual code in Collect.scala.  The code calls TwitterUtils in the Spark Streaming Twitter library to get a DStream of tweets.  Then, map is called to convert the tweets to JSON format.  Finally, call for each RDD on the DStream.  This example repartitions the RDD to write out so that you can control the number of output files.

```scala
 val tweetStream = TwitterUtils.createStream(ssc, Utils.getAuth)
  .map(gson.toJson(_))

tweetStream.foreachRDD((rdd, time) => {
  val count = rdd.count()
  if (count > 0) {
    val outputRDD = rdd.repartition(partitionsEachInterval)
    outputRDD.saveAsTextFile(
      outputDirectory + "/tweets_" + time.milliseconds.toString)
    numTweetsCollected += count
    if (numTweetsCollected > numTweetsToCollect) {
      System.exit(0)
    }
  }
})

```

Run [Collect.scala](scala/src/main/scala/com/databricks/apps/twitter_classifier/Collect.scala) yourself to collect a dataset of tweets:

```bash
 %  ${YOUR_SPARK_HOME}/bin/spark-submit \
     --class "com.databricks.apps.twitter_classifier.Collect" \
     --master ${YOUR_SPARK_MASTER:-local[4]} \
     target/scala-2.10/spark-twitter-lang-classifier-assembly-1.0.jar \
     ${YOUR_OUTPUT_DIR:-/tmp/tweets} \
     ${NUM_TWEETS_TO_COLLECT:-10000} \
     ${OUTPUT_FILE_INTERVAL_IN_SECS:-10} \
     ${OUTPUT_FILE_PARTITIONS_EACH_INTERVAL:-1} \
     --consumerKey ${YOUR_TWITTER_CONSUMER_KEY} \
     --consumerSecret ${YOUR_TWITTER_CONSUMER_SECRET} \
     --accessToken ${YOUR_TWITTER_ACCESS_TOKEN}  \
     --accessTokenSecret ${YOUR_TWITTER_ACCESS_SECRET}
```
