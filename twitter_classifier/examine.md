# Examine with Spark SQL

Spark SQL can be used to examine data based on the tweets.  Below are some relevant
code snippets from [ExamineAndTrain.scala](scala/src/main/scala/com/databricks/apps/twitter_classifier/ExamineAndTrain.scala).

First, here is code to pretty print 5 sample tweets so that they are
more human-readable.

```scala
val tweets = sc.textFile(tweetInput)
for (tweet <- tweets.take(5)) {
  println(gson.toJson(jsonParser.parse(tweet)))
}
```

Spark SQL can load JSON files and infer the schema based on that data.  Here is
the code to load the json files, register the data in the temp table called "tweetTable" and print out the schema based on that.

```scala
val tweetTable = sqlContext.jsonFile(tweetInput)
tweetTable.registerTempTable("tweetTable")
tweetTable.printSchema()
```

Now, look at the text of 10 sample tweets.
```scala
sqlContext.sql(
    "SELECT text FROM tweetTable LIMIT 10")
    .collect().foreach(println)
```

View the user language, user name, and text for 10 sample tweets.
```scala
sqlContext.sql(
    "SELECT user.lang, user.name, text FROM tweetTable LIMIT 10")
    .collect().foreach(println)
```

Finally, show the count of tweets by user language.  This can help determine the number of clusters that is ideal for this dataset of tweets.
```scala
sqlContext.sql(
    "SELECT user.lang, COUNT(*) as cnt FROM tweetTable " +
    "GROUP BY user.lang ORDER BY cnt DESC limit 1000")
    .collect.foreach(println)
```
