# Windowed Calculations

A typical use case for logs analysis is for monitoring a web server,
in which case we may only be interested in what's happened for the last one hour of time, and we want those statistics to refresh every minute.  One hour is called
the *window length*, while one minute is the *slide interval*.  In this
example, we use a window length of 30 seconds and a slide interval of
10 seconds as a comfortable choice for development.

The windows feature of Spark Streaming makes it very easy to compute
stats for a window of time, using the `window` function.

The first step is initalize the SparkConf and context objects, in particular a streaming context.  Note how only one Spark context is created from the conf and the streaming and sql contexts are created from those.  Next, the main body should be written.  Finally, the example calls ```start()``` on the streaming context, and ```awaitTermination() ```to keep the streaming context running.

```java
public class LogAnalyzerStreamingSQL {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Log Analyzer Streaming SQL");

    // Note: Only one Spark Context is created from the conf, the rest
    //       are created from the original Spark context.
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaStreamingContext jssc = new JavaStreamingContext(sc,
        SLIDE_INTERVAL);  // This sets the update window to be every 10 seconds.
    JavaSQLContext sqlContext = new JavaSQLContext(sc);

    // TODO: Insert code here to process logs.

    // Start the streaming server.
    jssc.start();              // Start the computation
    jssc.awaitTermination();   // Wait for the computation to terminate
  }
}
```

The first step of the main body is to create a DStream from reading the socket.
```java
JavaReceiverInputDStream<String> logDataDStream =
    jssc.socketTextStream("localhost", 9999);
```

Next, call the ```map``` transformation to convert the logDataDStream into a ApacheAccessLog DStream.
```java
JavaDStream<ApacheAccessLog> accessLogDStream =
    logDataDStream.map(ApacheAccessLog::parseFromLogLine).cache();
```

Next, call ```window``` on the accessLogDStream to create a windowed DStream.  The window function nicely packages the input data that is being
streamed into RDDs containing a window length of data, and creates a new
RDD every SLIDE_INTERVAL of time.
```java
JavaDStream<ApacheAccessLog> windowDStream =
    accessLogDStream.window(WINDOW_LENGTH, SLIDE_INTERVAL);
```

Then call ```foreachRDD``` on the windowDStream.  The function
passed into ```forEachRDD``` is called on each new RDD in the windowDStream as the RDD
is created, so every slide_interval.  The RDD passed into the function contains
all the input for the last window_length of time.  Now that we have
an RDD of ApacheAccessLogs, you can simply reuse code from either two batch examples (regular or SQL).  In this example, the code was just copied and pasted, but you could refactor this code into one place nicely for reuse in your production code base - no need to write new processing code just for streaming!
```java
windowDStream.foreachRDD(accessLogs -> {
  if (accessLogs.count() == 0) {
    System.out.println("No access logs in this time interval");
    return null;
  }
  // Insert code verbatim from LogAnalyzer.java or LogAnalyzerSQL.java here.

  // Calculate statistics based on the content size.
  JavaRDD<Long> contentSizes =
      accessLogs.map(ApacheAccessLog::getContentSize).cache();
  System.out.println(String.format("Content Size Avg: %s, Min: %s, Max: %s",
      contentSizes.reduce(SUM_REDUCER) / contentSizes.count(),
      contentSizes.min(Comparator.naturalOrder()),
      contentSizes.max(Comparator.naturalOrder())));

   //...Won't copy the rest here...
}
```

Now that we've walked through the code, run
 [LogAnalyzerStreaming.java](java8/src/main/java/com/databricks/apps/logs/chapter1/LogAnalyzerStreaming.java)
and/or [LogAnalyzerStreamingSQL.java](java8/src/main/java/com/databricks/apps/logs/chapter1/LogAnalyzerStreamingSQL.java) now.
