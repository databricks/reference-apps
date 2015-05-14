# Run Examine and Train

To run this program, the following argument list is required:

1. YOUR_TWEET_INPUT - This is the file pattern for input tweets.
* OUTPUT_MODEL_DIR - This is the directory to persist the model.
* NUM_CLUSTERS - The number of clusters the algorithm should create.
* NUM_ITERATIONS - The number of iterations the algorithm should run for.

Here is an example command to run [ExamineAndTrain.scala](scala/src/main/scala/com/databricks/apps/twitter_classifier/ExamineAndTrain.scala):

```bash
%  ${YOUR_SPARK_HOME}/bin/spark-submit \
     --class "com.databricks.apps.twitter_classifier.ExamineAndTrain" \
     --master ${YOUR_SPARK_MASTER:-local[4]} \
     target/scala-2.10/spark-twitter-lang-classifier-assembly-1.0.jar \
     "${YOUR_TWEET_INPUT:-/tmp/tweets/tweets*/part-*}" \
     ${OUTPUT_MODEL_DIR:-/tmp/tweets/model} \
     ${NUM_CLUSTERS:-10} \
     ${NUM_ITERATIONS:-20}
```
