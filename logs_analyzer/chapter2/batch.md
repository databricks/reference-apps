# Batch Data Import

This section covers batch importing data into Apache Spark, such as
seen in the non-streaming examples from Chapter 1.  Those examples load data
from files all at once into one RDD, processes that RDD, the job completes,
and the program exits.  In a production system, you could set up a cron job to
kick off a batch job each night to process the last day's worth of log files and then publish statistics for the last day.

## File Systems

To support batch import of data on a Spark cluster, the data needs to be accessible by all machines on the cluster.  Files that are only accessible on one worker machine
and cannot be read by the others will cause failures.

If you have a small dataset that can fit on one machine, you could manually copy your files onto all the nodes on your Spark cluster, perhaps using *rsync* to make that easier.

*NFS* or some other network file system makes sure all your machines can access the same files without require you to copy the files around.  But NFS isn't fault tolerant
to machine failures and if your dataset is too
big to fit on one NFS volume - you'd have to store the data on multiple volumes
and figure out which volume a particular file is on - which could get cumbersome.

HDFS and S3 are great file systems for massive datasets - built
to store a lot of data and give all the machines on the cluster
access to those files, while still being fault tolerant.  We give a few more tips on running Spark with these file systems since they are recommended.

* S3 is an Amazon AWS solution for storing files in the cloud, easily accessible to anyone who signs up for an account.
* HDFS is a distributed file system that is part of Hadoop and can be installed on your own datacenters.

The good news is that regardless of which of these file systems
you choose, you can run the same code to read from them - these
file systems are all "Hadoop compatible" file systems.

In this section, you should try running [LogAnalyzerBatchImport.java](java8/src/main/java/com/databricks/apps/logs/chapter2/LogAnalyzerBatchImport.java) on any files on your file system of choice.  There
is nothing new in this code - it's just a refactor of the [First Log Analyzer from Chapter One](../chapter1/spark.md).  Try passing in "*" or "?" for the textFile path, and Spark will read in all the files that match that pattern to create the RDD.

### S3

S3 is Amazon Web Services's solution for storing large files in the cloud.
On a production system, you want your Amazon EC2 compute nodes on the same zone
as your S3 files for speed as well as cost reasons.  While S3 files can be read
from other machines, it would take a long time and be expensive (Amazon S3 data
transfer prices differ if you read data within AWS vs. to somewhere else on the
internet).

See [running Spark on EC2](https://spark.apache.org/docs/latest/ec2-scripts.html)
if you want to launch a Spark cluster on AWS - charges may apply.

If you choose to run this example with a local Spark cluster on your machine rather
than EC2 compute nodes to read the files in S3, use a small data input source.

1. Sign up for an [Amazon Web Services](https://aws.amazon.com/) Account.
* Load an example logs file to s3.
  * Log into the [AWS console for S3](https://console.aws.amazon.com/s3/)
  * Create an S3 bucket.
  * Upload a couple of example log files to that bucket.
  * Your files will be at the path: s3n://YOUR_BUCKET_NAME/YOUR_LOGFILE.log
* Configure your security credentials for AWS:
  * Create and [download your security credentials](https://console.aws.amazon.com/iam/home?#security_credential)
  * Set the environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to the correct values on all machines on your cluster.  These can also be set in the SparkConf object programmatically.

Now, run [LogAnalyzerBatchImport.java](java8/src/main/java/com/databricks/apps/logs/chapter2/LogAnalyzerBatchImport.java)
passing in the s3n path to your files.


### HDFS

HDFS is a file system that is meant for storing large data sets and being fault
tolerant.  In a production system, your Spark cluster
should ideally be on the same machines as your Hadoop cluster to make it easy to
read files.  The Spark binary you run on your clusters must be compiled with the
same HDFS version as the one you wish to use.

There are many ways to install HDFS, but heading to the [Hadoop homepage](http://hadoop.apache.org/)
is one way to get started.

Run [LogAnalyzerBatchImport.java](java8/src/main/java/com/databricks/apps/logs/chapter2/LogAnalyzerBatchImport.java)
on any file pattern on your hdfs directory.

## Databases

Spark is also compatible with many databases for import and more may
be added soon - we won't go over them all here - refer to the
[Apache Spark Examples Directory](https://github.com/apache/spark/tree/master/examples)
which contains more examples that may be useful to you.  Most likely, you
aren't going to be reading your logs data in from a databases, but there
may be other data you want to input to Spark.
