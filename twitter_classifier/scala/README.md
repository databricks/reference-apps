# Spark-Twitter-Lang-Classifier
This program was built for Spark 2.0.1.

## Registering An App with Twitter
The 3 programs in this project all share the same Twitter application ID.
They do not create tweets, so they only need Twitter read privileges.
[Obtain Twitter application credentials](https://apps.twitter.com/) associated your Twitter account.
The values provided by this process must be set in bash environment variables, like this:

    export SPARK_TWITTER_CONSUMER_KEY=blahblah
    export SPARK_TWITTER_CONSUMER_SECRET=blahblah
    export SPARK_TWITTER_ACCESS_TOKEN=blahblah
    export SPARK_TWITTER_ACCESS_TOKEN_SECRET=blahblah

## Running from SBT
This is only useful for checking out help messages and testing command line parsing.

    $ sbt "runMain com.databricks.apps.twitterClassifier.Collect \
     --consumerKey $SPARK_TWITTER_CONSUMER_KEY \
     --consumerSecret $SPARK_TWITTER_CONSUMER_SECRET \
     --accessToken $SPARK_TWITTER_ACCESS_TOKEN \
     --accessTokenSecret $SPARK_TWITTER_ACCESS_TOKEN_SECRET \
     --overWrite \
     ~/sparkTwitter/data 1000 0.1 5"
    
## Assembling the Twitter Classifier Assembly
A "fat jar" needs to be built from the source code and dependencies.
Upon successfully building the assembly, you should be able to run the various Spark jobs as documented in the [Gitbook](https://www.gitbook.io/read/book/databricks/databricks-spark-reference-applications).

To build the assembly, run SBT using the `assembly` target: 

    $ ./sbt/sbt clean assembly

The generated file will be called something like
`target/scala-2.11/spark-twitter-lang-classifier-assembly-2.0.0.jar`

## Running Locally

See the `bin/collect` script. Still need to write similar scripts for `ExamineAndTrain` and `Predict`.

    # Usage: ExamineAndTrain <tweetDirectory> <outputModelDir> <numClusters> <numIterations>
    spark-shell --master local[*] \
    --class com.databricks.apps.twitterClassifier.ExamineAndTrain \
    --jars target/scala-2.11/spark-twitter-lang-classifier-assembly-2.0.0.jar \
    ~/sparkTwitter/data ~/sparkTwitter/model 5 10

    # Usage: Predict <modelDirectory> <clusterNumber>
    spark-shell --master local[*] \
    --class com.databricks.apps.twitterClassifier.Predict \
    --jars target/scala-2.11/spark-twitter-lang-classifier-assembly-2.0.0.jar \
    ~/sparkTwitter/model 5
