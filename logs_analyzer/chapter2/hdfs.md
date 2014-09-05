# HDFS

HDFS is a file system that is meant for storing large data sets and being fault
tolerant.  In a production system, your Spark cluster
should ideally be on the same machines as your Hadoop cluster to make it easy to
read files.  The Spark binary you run on your clusters must be compiled with the
same HDFS version as the one you wish to use.

There are many ways to install HDFS, but heading to the [Hadoop homepage](http://hadoop.apache.org/)
is one way to get started and run hdfs locally on your machine.

Run [LogAnalyzerBatchImport.java](java8/src/main/java/com/databricks/apps/logs/chapter2/LogAnalyzerBatchImport.java)
on any file pattern on your hdfs directory.
