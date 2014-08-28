# Chapter 1: Introduction to Apache Spark

In this chapter, we demonstrate how simple it is to analyze web logs using
Apache Spark.  We'll show how to load a Resilient Distributed Dataset
(**RDD**) of access log lines and use Spark tranformations and actions to compute
some statistics for web server monitoring.  In the process, we'll introduce
the Spark SQL and the Spark Streaming libraries.

In this explanation, the code snippets are in Java 8.  However,
there is also sample code in [Java 6](java6), [Scala](scala), and [Python](python)
included in this directory.  Refer to the README's in those folders for
instructions on how to build and run those examples.

This chapter covers the following topics:

 1. [First Log Analyzer](spark.md) - This is a first Spark standalone logs analysis application.
 * [Spark SQL](sql.md) - This example does the same thing as the above example, but uses SQL syntax instead of Spark transformations and actions.
 * [Spark Streaming](streaming.md) - This example covers how to calculate log statistics using the streaming library.
