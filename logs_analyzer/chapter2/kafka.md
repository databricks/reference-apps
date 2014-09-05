# Kafka

While the previous example picks up new log files right away - the log
files aren't copied over until a long time after the HTTP requests in the logs
actually occurred.  While that enables auto-refresh of log data, that's still not realtime.  To get realtime
logs processing, we need a way to send over log lines immediately.  Kafka is a
high-throughput distributed message system that is perfect for that use case.  Spark
contains an external module importing data from Kafka.

Here is some useful documentation to set up Kafka for Spark Streaming:

* [Kafka Documentation](http://kafka.apache.org/documentation.html)
* [KafkaUtils class in the external module of the Spark project](https://github.com/apache/spark/blob/master/external/kafka/src/main/scala/org/apache/spark/streaming/kafka/KafkaUtils.scala) - This is the external module that has been written that imports data from Kafka into Spark Streaming.
* [Spark Streaming Example of using Kafka](https://github.com/apache/spark/blob/master/examples/src/main/java/org/apache/spark/examples/streaming/JavaKafkaWordCount.java) - This is an example that demonstrates how to call KafkaUtils.
