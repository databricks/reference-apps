# Spark Streaming

Familiarize yourself with the concepts introducted in the
[Spark Streaming Programming Guide](https://spark.apache.org/docs/latest/streaming-programming-guide.html)
before beginning this section.  In particular, it defines DStreams which are an
essential concept in understanding Spark Streaming.

The earlier examples show us how to compute statistics on a log file that is no longer being modified - but
in a production system, we want live monitoring of the logs.
To do that, we need to use Spark Streaming.

To run the streaming examples, tail a log file into netcat to send to Spark.
This is not the ideal way to get data into Spark in a production system,
but is an easy workaround for a first Spark Streaming example.  We will cover best practices for how to import data for Spark
Streaming in [Chapter 2](../chapter2/README.md).

In a terminal window, just run this command on a logfile which you will append to:
```
% tail -f [[YOUR_LOG_FILE]] | nc -lk 9999
```

If you don't have a live log file that is being updated on the fly, you
can add lines manually with the included data file or another your own log file:
```
% cat ../../data/apache.access.log >> [[YOUR_LOG_FILE]]
```

When data is streamed into Spark, there are two common use cases covered:

1. [Windowed Calculations](windows.md) means that you only care about data
received in the last N amount of time.  Perhaps when monitoring your web servers,
you only care about what has happened in the last hour.
   * Spark Streaming conveniently
splits the input data into the desired time windows for easy processing, using the `window` function of the streaming library.
   * The `forEachRDD` function allows you to access the RDD's created each time interval.
1. [Cumulative Calculations](total.md) means that you want to keep cumulative
statistics, while streaming in new data to refresh those statistics.  In that
case, you need to maintain the state for those statistics.
   * The Spark
Streaming library has some convenient functions for maintaining state
to support this use case, `updateStateByKey`.
   * `tranform` is also introduced to allow code reuse.
