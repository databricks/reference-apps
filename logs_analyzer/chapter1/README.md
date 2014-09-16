# Section 1: Introduction to Apache Spark

In this section, we demonstrate how simple it is to analyze web logs using
Apache Spark.  We'll show how to load a Resilient Distributed Dataset
(**RDD**) of access log lines and use Spark tranformations and actions to compute
some statistics for web server monitoring.  In the process, we'll introduce
the Spark SQL and the Spark Streaming libraries.

In this explanation, the code snippets are in [Java 8](https://github.com/databricks/reference-apps/tree/master/logs_analyzer/chapter1/java8).  However,
there is also sample code in [Java 6](https://github.com/databricks/reference-apps/tree/master/logs_analyzer/chapter1/java6), [Scala](https://github.com/databricks/reference-apps/tree/master/logs_analyzer/chapter1/scala), and [Python](https://github.com/databricks/reference-apps/tree/master/logs_analyzer/chapter1/python)
included in this directory.  In those folders are README's for
instructions on how to build and run those examples, and the necessary build files with all the required dependencies.

This chapter covers the following topics:

 1. [First Log Analyzer in Spark](spark.md) - This is a first Spark standalone logs analysis application.
 * [Spark SQL](sql.md) - This example does the same thing as the above example, but uses SQL syntax instead of Spark transformations and actions.
 * [Spark Streaming](streaming.md) - This example covers how to calculate log statistics using the streaming library.
