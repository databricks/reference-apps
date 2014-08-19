# Streaming Data Import

This section covers importing data for streaming.  The streaming example in the
previous chapter received data through a single socket - which is not
a scalable solution.  In a real production system, there are many servers
continuously writing logs, and we want to process all of those files.  This
section contains scalable solutions for data import.  Since streaming is now
used, there is no longer the need for a nightly batch job to process logs,
but instead - this logs processing program can be long-lived - continuously
receiving new logs data, processing the data, and computing log stats.

## Built in Methods for Streaming Import

The StreamingContext has many built in methods for importing data for streaming.
We used ```socketTextStream``` in the previous chapter, and we'll use ```textFileStream```
here.  The ```textFileStream``` method monitors any Hadoop-compatible filesystem directory for new
files and when it detects a new file - reads it into Spark Streaming.
Just replace the call to ```socketTextStream``` with ```textFileStream```,
and pass in the directory to monitor for log files.

```java
// This methods monitors a directory for new files
// to read in for streaming.
JavaDStream<String> logData = jssc.textFileStream(directory);
```

Try running [LogAnalyzerStreamingImportDirectory.java](java8/src/main/java/com/databricks/apps/logs/chapter2/LogAnalyzerStreamingImportDirectory.java)
by specifying a directory.   You'll also need to drop or copy some new log files
into that directory while the program is running.

There are more built-in input methods for streaming - check them out in the
reference API documents for the StreamingContext.

## Kafka

While the previous example picks up new log files right away - the log
files aren't copied over until a long time after the HTTP requests in the logs
actually occurred, so we don't get up to date log statistics.  To get real time
logs processing, we need a way to send over log lines immediately.  Kafka is a
high-throughput distributed message system that is perfect for that use case.  There
is an external module of Spark for Kafka import.

Here is some useful documentation to set up Kafka for Spark Streaming:

* [Kafka Documentation](http://kafka.apache.org/documentation.html)
* [KafkaUtils class in the external module of the Spark project](https://github.com/apache/spark/blob/master/external/kafka/src/main/scala/org/apache/spark/streaming/kafka/KafkaUtils.scala) - This is the external module that has been written that imports data from Kafka into Spark Streaming.
* [Spark Streaming Example of using Kafka](https://github.com/apache/spark/blob/master/examples/src/main/java/org/apache/spark/examples/streaming/JavaKafkaWordCount.java) - This is an example that demonstrates how to call KafkaUtils.
