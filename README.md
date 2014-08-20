# Databricks Reference Apps

At Databricks, we are developing a set of reference applications that demonstrate how to use Apache Spark.  This book will explains them.  All of the code mentioned for these reference applications can be found [here](https://github.com/databricks/reference-apps).  This documentation is also published in an online book format [here](http://databricks.gitbooks.io/databricks-spark-reference-applications/).

The reference applications will appeal to those who want to learn Spark and learn better by example.  Browse the applications, see
what features of the reference applications are similar to the features you want to build, and
refashion the code samples for your needs.  Additionally, this is meant to be a practical guide for using Spark in your
systems, so the applications mention other technologies that are compatible with Spark - such as what file systems to use for storing your massive data sets.

* [Log Analyzer Application](log_analyzer/README.md) - The logs analyzer reference application contains a series of tutorials for learning Spark by example as well as a final application that can be used to monitor Apache access logs.

* [Twitter Streaming Language Classifier](twitter_classifier/README.md) - This application demonstrates how to fetch and train a classifier of Tweets using Spark.  Then Spark Streaming is used to call the trained classifier and filter out live tweets.

