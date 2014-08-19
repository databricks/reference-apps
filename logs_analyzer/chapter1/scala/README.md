# Chapter 1: Introduction to Apache Spark in Scala.

This directory contains all the sample code for Chapter 1 in Scala.  For
more explanation of this code, see (Chapter 1)[../README.md].

To compile this code, use sbt:
```
% sbt package
```

To run an example, such as LogAnalyzer, you can use spark-submit program
```
%  ${YOUR_SPARK_HOME}/bin/spark-submit \
   --class "com.databricks.apps.logs.chapter1.LogAnalyzer" \
   --master local[4] \
   target/scala-2.10/spark-logs-analyzer_2.10-1.0.jar \
   ../../data/apache.access.log
```

Or you can run the program in your IDE by setting a JVM Parameter
```
-Dspark.master=local[4]
```

Note: It is very helpful to set the log4j logging level to be WARN when
running these examples..