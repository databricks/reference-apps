# Chapter 1: Introduction to Apache Spark in Java 8.

This directory contains all the sample code for Chapter 1 in Java 8.

To compile this code, use maven:
```
% mvn package
```

To run an example, such as LogAnalyzer, you can use spark-submit program
```
%  ${YOUR_SPARK_HOME}/bin/spark-submit
   --class "com.databricks.apps.logs.chapter1.LogAnalyzer"
   --master local[4]
   target/log-analyzer-1.0.jar
   ../../data/apache.access.log
```

Or you can run the program in your IDE by setting a JVM Parameter
```
-Dspark.master=local[4]
```

Note: It is very helpful to set the log4j logging level to be WARN when
running these examples..
