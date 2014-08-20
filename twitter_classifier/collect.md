# Part 1: Collect a Dataset of Tweets

Spark Streaming is used to collect tweets as the dataset.  The tweets are written out in json format, one tweet per line.  A file of tweets is written every time interval until at least the desired number of tweets is collected.

See [Collect.scala](scala/src/main/scala/com/databricks/apps/twitter_classifier/Collect.scala) for the full code.  We'll walk through some
of the interesting bits now.

Collect.scala takes in the following argument list:

1. *outputDirectory* - the output directory for writing the tweets.  The files will be named 'part-%05d'
*  *intervalInSeconds* - write out a new file of tweets every interval.
*  *numTweetsToCollect* - this is the minimum number of tweets to collect before the program exits.

Collect.scala will also require [Twitter API Credentials](https://apps.twitter.com/).  If you have never signed up for a Twitter Api Credentials, follow these steps [here](https://databricks-training.s3.amazonaws.com/realtime-processing-with-spark-streaming.html#twitter-credential-setup).  The Twitter credentials are passed in through command line flags.

Below is an abbreviated snippet of the actual code in Collect.scala.  The code calls TwitterUtils in the Spark Streaming Twitter library to get a DStream of tweets.  We then call foreachRDD on the DStream, which will be called on the RDD of tweets which gets created each time interval.  We can then create an output file, collect all the tweets, and write them all out to file in json format.

```scala
val tweetStream = TwitterUtils.createStream(ssc, Utils.getAuth)

tweetStream.foreachRDD(rdd => {
  val count = rdd.count()
  if (count > 0) {
    val writer = new PrintWriter("%s/part-%05d".format(outputDirectory,
        partNum), "UTF-8")
    val tweetJsonArray = rdd.collect()
    for (tweetJson <- tweetJsonArray) {
      writer.println(gson.toJson(tweetJson))
    }
    writer.close()
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
     ${OUTPUT_FILE_INTERVAL_IN_SECS:-10} \
     ${NUM_TWEETS_TO_COLLECT:-10000} \
     --consumerKey ${YOUR_TWITTER_CONSUMER_KEY} \
     --consumerSecret ${YOUR_TWITTER_CONSUMER_SECRET} \
     --accessToken ${YOUR_TWITTER_ACCESS_TOKEN}  \
     --accessTokenSecret ${YOUR_TWITTER_ACCESS_SECRET}
```
