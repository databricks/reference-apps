# Databricks Reference Apps

At Databricks, we are developing a set of reference applications that demonstrate how to use Apache Spark.  This book/repo contains the reference applications.

* View the code in the Github Repo here: [https://github.com/databricks/reference-apps](https://github.com/databricks/reference-apps)
* Read the documentation here: [http://databricks.gitbooks.io/databricks-spark-reference-applications/](http://databricks.gitbooks.io/databricks-spark-reference-applications/)
* Submit feedback or issues here: [https://github.com/databricks/reference-apps/issues](https://github.com/databricks/reference-apps/issues)

The reference applications will appeal to those who want to learn Spark and learn better by example.  Browse the applications, see
what features of the reference applications are similar to the features you want to build, and
refashion the code samples for your needs.  Additionally, this is meant to be a practical guide for using Spark in your
systems, so the applications mention other technologies that are compatible with Spark - such as what file systems to use for storing your massive data sets.

* [Log Analysis Application](http://databricks.gitbooks.io/databricks-spark-reference-applications/content/logs_analyzer/index.html) - The log analysis reference application contains a series of tutorials for learning Spark by example as well as a final application that can be used to monitor Apache access logs.  The examples use Spark in batch mode, cover Spark SQL, as well as Spark Streaming.

* [Twitter Streaming Language Classifier](http://databricks.gitbooks.io/databricks-spark-reference-applications/content/twitter_classifier/index.html) - This application demonstrates how to fetch and train a language classifier for Tweets using Spark MLlib.  Then Spark Streaming is used to call the trained classifier and filter out live tweets that match a specified cluster. For directions on how to build and run this app - see [twitter_classifier/scala/README.md](https://github.com/databricks/reference-apps/blob/master/twitter_classifier/scala/README.md).

* [Weather TimeSeries Data Application with Cassandra](http://databricks.gitbooks.io/databricks-spark-reference-applications/content/timeseries/index.html) - This reference application works with Weather Data which is taken for a given weather station at a given point in time.  The app demonstrates several strategies for leveraging Spark Streaming integrated with Apache Cassandra and Apache Kafka for fast, fault-tolerant, streaming computations with time series data.

These reference apps are covered by license terms covered [here](http://databricks.gitbooks.io/databricks-spark-reference-applications/content/LICENSE).
