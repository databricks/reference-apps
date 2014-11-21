# Databricks Reference Apps

At Databricks, we are developing a set of reference applications that demonstrate how to use Apache Spark.  This book/repo contains the reference applications.

* View the code in the Github Repo here: [https://github.com/databricks/reference-apps](https://github.com/databricks/reference-apps)
* Read the documentation here: [http://databricks.gitbooks.io/databricks-spark-reference-applications/](http://databricks.gitbooks.io/databricks-spark-reference-applications/)
* Submit feedback or issues here: [https://github.com/databricks/reference-apps/issues](https://github.com/databricks/reference-apps/issues)

The reference applications will appeal to those who want to learn Spark and learn better by example.  Browse the applications, see
what features of the reference applications are similar to the features you want to build, and
refashion the code samples for your needs.  Additionally, this is meant to be a practical guide for using Spark in your
systems, so the applications mention other technologies that are compatible with Spark - such as what file systems to use for storing your massive data sets.

* [Log Analysis Application](http://databricks.gitbooks.io/databricks-spark-reference-applications/content/logs_analyzer/README.html) - The log analysis reference application contains a series of tutorials for learning Spark by example as well as a final application that can be used to monitor Apache access logs.  The examples use Spark in batch mode, cover Spark SQL, as well as Spark Streaming.

* [Twitter Streaming Language Classifier](http://databricks.gitbooks.io/databricks-spark-reference-applications/content/twitter_classifier/README.html) - This application demonstrates how to fetch and train a language classifier for Tweets using Spark MLLib.  Then Spark Streaming is used to call the trained classifier and filter out live tweets that match a specified cluster. To build this example go into the twitter_classifier/scala and follow the direction in the README.

* [Streaming Kafka, Cassandra and Akka TimeSeries Application] - The time series reference application (constantly improving) demonstrates several strategies to easily leverage and integrate Apache Spark, Spark Streaming, Apache Cassandra, and Apache Kafka for fast, fault tolerant, streaming computations in asynchronous Akka event-driven environments. This application focuses on the use case of time series data. To build and run this example go into the time_series/scala and follow the direction in the README.

This reference app is covered by license terms covered [here](http://databricks.gitbooks.io/databricks-spark-reference-applications/content/LICENSE).