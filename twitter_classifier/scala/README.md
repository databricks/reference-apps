# Spark-Twitter-Lang-Classifier
This program was built for Spark 2.0.1.

## Registering An App with Twitter
The programs in this project which interact with Twitter all share the same Twitter application ID.
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

    $ sbt/sbt clean assembly

The generated file will be called something like
`target/scala-2.11/spark-twitter-lang-classifier-assembly-2.0.0.jar`

## Running Locally
 * `bin/sbt` fetches and runs SBT so you can compile and assemble the programs.
 * `bin/options` contains settings for the following scripts.
   * `bin/collect` stores tweets in the `~/sparkTwitter/data/` subdirectory.
   * `bin/train` reads the tweets in the `~/sparkTwitter/data/` subdirectory, then stores a model into the `~/sparkTwitter/model/` subdirectory.
   * `bin/predict` uses the model to filter the stream.
