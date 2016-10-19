# Cumulative Calculations: updateStateByKey()

To keep track of the log statistics for all of time, state must be maintained between processing RDD's in a DStream.

To maintain state for key-pair values, the data may be too big to fit in memory on one machine - Spark Streaming can maintain the state for you.  To do that, call the
```updateStateByKey``` function of the Spark Streaming library.

First, in order to use ```updateStateByKey```, checkpointing must be enabled on the streaming context.  To do that, just call ```checkpoint```
on the streaming context with a directory to write the checkpoint data. Here is
part of the main function of a streaming application that will save state for all of time:
```java
public class LogAnalyzerStreamingTotal {
  public static void main(String[] args) throws InterruptedException {
    SparkConf conf = new SparkConf().setAppName("Log Analyzer Streaming Total");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaStreamingContext jssc = new JavaStreamingContext(sc,
        new Duration(10000));  // This sets the update window to be every 10 seconds.

    // Checkpointing must be enabled to use the updateStateByKey function.
    jssc.checkpoint("/tmp/log-analyzer-streaming");

    // TODO: Insert code for computing log stats.

    // Start the streaming server.
    jssc.start();              // Start the computation
    jssc.awaitTermination();   // Wait for the computation to terminate
```

To compute the content size statistics, simply use static variables
to save the current running sum, count, min and max of the content sizes.

```java
// These static variables stores the running content size values.
private static final AtomicLong runningCount = new AtomicLong(0);
private static final AtomicLong runningSum = new AtomicLong(0);
private static final AtomicLong runningMin = new AtomicLong(Long.MAX_VALUE);
private static final AtomicLong runningMax = new AtomicLong(Long.MIN_VALUE);
```

To update those values, first call map on the AccessLogDStream to retrieve a contentSizeDStream.  Then just update the values for the static variables by calling
foreachRDD on the contentSizeDstream, and calling actions on the RDD:
```java
JavaDStream<Long> contentSizeDStream =
    accessLogDStream.map(ApacheAccessLog::getContentSize).cache();
contentSizeDStream.foreachRDD(rdd -> {
  if (rdd.count() > 0) {
    runningSum.getAndAdd(rdd.reduce(SUM_REDUCER));
    runningCount.getAndAdd(rdd.count());
    runningMin.set(Math.min(runningMin.get(), rdd.min(Comparator.naturalOrder())));
    runningMax.set(Math.max(runningMax.get(), rdd.max(Comparator.naturalOrder())));
    System.out.print("Content Size Avg: " +  runningSum.get() / runningCount.get());
    System.out.print(", Min: " + runningMin.get());
    System.out.println(", Max: " + runningMax.get());
  }
});
```

For the other statistics, since they make use of key value pairs, static variables
can't be used anymore.  The amount of state that needs to be maintained
is potentially too big to fit in memory.  So
for those stats, we'll make use of ```updateStateByKey``` so Spark streaming will maintain
a value for every key in our dataset.

But before we can call ```updateStateByKey```, we need to create a function to pass into it.  ```updateStateByKey``` takes in a different reduce function.
While our previous sum reducer just took in two values and output their sum, this
reduce function takes in a current value and an iterator of values,
and outputs one new value.
```java
private static Function2<List<Long>, Optional<Long>, Optional<Long>>
   COMPUTE_RUNNING_SUM = (nums, current) -> {
     long sum = current.or(0L);
     for (long i : nums) {
       sum += i;
     }
     return Optional.of(sum);
   };
```

Finally, we can compute the keyed statistics for all of time with this code:
```java
// Compute Response Code to Count.
// Note the use of updateStateByKey.
JavaPairDStream<Integer, Long> responseCodeCountDStream = accessLogDStream
    .mapToPair(s -> new Tuple2<>(s.getResponseCode(), 1L))
    .reduceByKey(SUM_REDUCER)
    .updateStateByKey(COMPUTE_RUNNING_SUM);
responseCodeCountDStream.foreachRDD(rdd -> {
  System.out.println("Response code counts: " + rdd.take(100));
});

// A DStream of ipAddresses accessed > 10 times.
JavaDStream<String> ipAddressesDStream = accessLogDStream
    .mapToPair(s -> new Tuple2<>(s.getIpAddress(), 1L))
    .reduceByKey(SUM_REDUCER)
    .updateStateByKey(COMPUTE_RUNNING_SUM)
    .filter(tuple -> tuple._2() > 10)
    .map(Tuple2::_1);
ipAddressesDStream.foreachRDD(rdd -> {
  List<String> ipAddresses = rdd.take(100);
  System.out.println("All IPAddresses > 10 times: " + ipAddresses);
});

// A DStream of endpoint to count.
JavaPairDStream<String, Long> endpointCountsDStream = accessLogDStream
    .mapToPair(s -> new Tuple2<>(s.getEndpoint(), 1L))
    .reduceByKey(SUM_REDUCER)
    .updateStateByKey(COMPUTE_RUNNING_SUM);
endpointCountsDStream.foreachRDD(rdd -> {
  List<Tuple2<String, Long>> topEndpoints =
      rdd.top(10, new ValueComparator<>(Comparator.<Long>naturalOrder()));
  System.out.println("Top Endpoints: " + topEndpoints);
});
```

Run [LogAnalyzerStreamingTotal.java](java8/src/main/java/com/databricks/apps/logs/chapter1/LogAnalyzerStreamingTotal.java)
now for yourself.
