# Importing from Files

To support batch import of data on a Spark cluster, the data needs to be accessible by all machines on the cluster.  Files that are only accessible on one worker machine
and cannot be read by the others will cause failures.

If you have a small dataset that can fit on one machine, you could manually copy your files onto all the nodes on your Spark cluster, perhaps using `rsync` to make that easier.

**NFS** or some other network file system makes sure all your machines can access the same files without requiring you to copy the files around.  But NFS isn't fault tolerant
to machine failures and if your dataset is too
big to fit on one NFS volume - you'd have to store the data on multiple volumes
and figure out which volume a particular file is on - which could get cumbersome.

**HDFS** and **S3** are great file systems for massive datasets - built
to store a lot of data and give all the machines on the cluster
access to those files, while still being fault tolerant.  We give a few more tips on running Spark with these file systems since they are recommended.

* [S3](s3.md) is an Amazon AWS solution for storing files in the cloud, easily accessible to anyone who signs up for an account.
* [HDFS](hdfs.md) is a distributed file system that is part of Hadoop and can be installed on your own datacenters.

The good news is that regardless of which of these file systems
you choose, you can run the same code to read from them - these
file systems are all "Hadoop compatible" file systems.

In this section, you should try running [LogAnalyzerBatchImport.java](java8/src/main/java/com/databricks/apps/logs/chapter2/LogAnalyzerBatchImport.java) on any files on your file system of choice.  There
is nothing new in this code - it's just a refactor of the [First Log Analyzer from Chapter One](../chapter1/spark.md).  Try passing in "*" or "?" for the textFile path, and Spark will read in all the files that match that pattern to create the RDD.

