# Spark-Twitter-Lang-Classifier
The default setting is to use Spark 2.0.1. 
You should able to run this coding with other versions of Spark 2.+ by changing the Spark version in the build file (assuming no API changes).

## Running from SBT
This is only useful for checking out help messages and testing command line parsing.

    $ sbt "runMain com.databricks.apps.twitterClassifier.Collect \
     --consumerKey blahblah \
     --consumerSecret blahblah \
     --accessToken blahblah \
     --accessTokenSecret blahblah \
     ~/sparkTwitter/data 1000 0.1 5"
    
## Assembling the Twitter Classifier Assembly
A "fat jar" needs to be built from the source code and dependencies.
Upon successfully building the assembly, you should be able to run the various Spark jobs as documented in the [Gitbook](https://www.gitbook.io/read/book/databricks/databricks-spark-reference-applications).

To build the assembly, run SBT using the `assembly` target: 

    $ ./sbt/sbt clean assembly

The generated file will be called something like
`target/scala-2.11/spark-twitter-lang-classifier-assembly-2.0.0.jar`

## Running Locally

    # Usage: Collect <outputDirectory> <numTweetsToCollect> <intervalInSeconds> <partitionsEachInterval>
    spark-shell --master local \
    --class com.databricks.apps.twitterClassifier.Collect\
    --jars target/scala-2.11/spark-twitter-lang-classifier-assembly-2.0.0.jar \
    --consumerKey blahblah \
    --consumerSecret blahblah \
    --accessToken blahblah \
    --accessTokenSecret blahblah \
    ~/sparkTwitter/data 1000 0.1 5

    # Usage: ExamineAndTrain <tweetInput> <outputModelDir> <numClusters> <numIterations>
    spark-shell --master local \
    --class com.databricks.apps.twitterClassifier.ExamineAndTrain \
    --jars target/scala-2.11/spark-twitter-lang-classifier-assembly-2.0.0.jar \
    ~/sparkTwitter/data ~/sparkTwitter/model 5 10

    # Usage: Predict <modelDirectory> <clusterNumber>
    spark-shell --master local \
    --class com.databricks.apps.twitterClassifier.Predict \
    --jars target/scala-2.11/spark-twitter-lang-classifier-assembly-2.0.0.jar \
    ~/sparkTwitter/model 5
