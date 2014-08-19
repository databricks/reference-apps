# Cumulative Calculations

If you really want to keep track of the log statistics for all of time, you'll
need to maintain the state of the statistics between processing RDD's.  Spark
Streaming doesn't keep all the data for all of time or there would be no more memory - it knows when an RDD is no longer needed and drops those objects.

If you wish to compute keyed statistics and the number of keys is very large
(i.e. too big to fit in memory on one machine), you can't maintain all the
state you are trying to save in your main program - you'll need Spark to maintain the state for you.  To do that, use the
```updateStateByKey``` function of the Spark Streaming library.

First, to use ```updateStateByKey```, we need to set up checkpointing on the streaming
context because we are maintaining state.  To do that, just call ```checkpoint```
on the streaming context with a directory to write the checkpoint data:
```java
public class LogAnalyzerStreamingTotal {
  public static void main(String[] args) {
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

To compute the content size statistics, we can simply use static variables
to save the current running sum, count, min and max of the content sizes.

```java
// These static variables stores the running content size values.
private static final AtomicLong runningCount = new AtomicLong(0);
private static final AtomicLong runningSum = new AtomicLong(0);
private static final AtomicLong runningMin = new AtomicLong(Long.MAX_VALUE);
private static final AtomicLong runningMax = new AtomicLong(Long.MIN_VALUE);
```

We call map on the AccessLogDStream to retrieve a contentSizeDStream.
Now, we can just update the values for these static variables by calling
foreachRDD on the contentSizeDstream:
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
  return null;
});
```

For the other statistics, since they make use of key value pairs, we can't
use static variables anymore.  The amount of state what we need to maintain
is potentially too big to fit in memory.  So
for those stats, we'll make use of ```updateStateByKey``` which allows us to maintain
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

Now, we can compute the keyed statistics for all of time:
```java
// Compute Response Code to Count.
// Note the use of updateStateByKey.
JavaPairDStream<Integer, Long> responseCodeCountDStream = accessLogDStream
    .mapToPair(s -> new Tuple2<>(s.getResponseCode(), 1L))
    .reduceByKey(SUM_REDUCER)
    .updateStateByKey(COMPUTE_RUNNING_SUM);
responseCodeCountDStream.foreachRDD(rdd -> {
  System.out.println("Response code counts: " + rdd.take(100));
  return null;
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
  return null;
});

// A DStream of endpoint to count.
JavaPairDStream<String, Long> endpointCountsDStream = accessLogDStream
    .mapToPair(s -> new Tuple2<>(s.getEndpoint(), 1L))
    .reduceByKey(SUM_REDUCER)
    .updateStateByKey(COMPUTE_RUNNING_SUM);
endpointCountsDStream.foreachRDD(rdd -> {
  List<Tuple2<String, Long>> topEndpoints =
      rdd.takeOrdered(10, new ValueComparator<>(Comparator.<Long>naturalOrder()));
  System.out.println("Top Endpoints: " + topEndpoints);
  return null;
});
```

Run [LogAnalyzerStreamingTotal.java](java8/src/main/java/com/databricks/apps/logs/chapter1/LogAnalyzerStreamingTotal.java)
now for yourself.

## The `transform` functions

As you may have noticed, while the functions you called on a DStream
is named the same as those you called on an RDD in the batch example,
it may not be clear how to reuse the code from the batch examples.

DStreams have `tranform` functions which allows you to call
any arbitrary RDD to RDD functions to RDD's in the DStream.  The
`tranform` functions are perfect for reusing any RDD to RDD functions
that you may have written in batch code and want to port over to
streaming.  Let's look at some code to illustrate this point.

Let's say we have separated out a function, `responseCodeCount` from our
batch example that can compute the response code count given the
apache access logs RDD:
```java
public static JavaPairRDD<Integer, Long> responseCodeCount(
   JavaRDD<ApacheAccessLog> accessLogRDD) {
  return accessLogRDD
     .mapToPair(s -> new Tuple2<>(s.getResponseCode(), 1L))
     .reduceByKey(SUM_REDUCER);
}
```

The responseCodeDStream can be created by calling `transformToPair` with the `responseCodeCount` function to the apacheAccessLogsDStream.
Then, you can finish up by calling `updateStateByKey` to keep a running count of the response codes for all of time,
and use `forEachRDD` to print the values out:

```java
// Compute Response Code to Count.
// Notice the user transformToPair to produce the a DStream of
// response code counts, and then updateStateByKey to accumulate
// the response code counts for all of time.
JavaPairDStream<Integer, Long> responseCodeCountDStream = accessLogDStream
   .transformToPair(LogAnalyzerStreamingTotalRefactored::responseCodeCount);
JavaPairDStream<Integer, Long> cumulativeResponseCodeCountDStream =
   responseCodeCountDStream.updateStateByKey(COMPUTE_RUNNING_SUM);
cumulativeResponseCodeCountDStream.foreachRDD(rdd -> {
  System.out.println("Response code counts: " + rdd.take(100));
  return null;
});
```

It is possible to combine `transform` functions before and after an
`updateStateByKey` as well:
```java
// A DStream of ipAddresses accessed > 10 times.
JavaDStream<String> ipAddressesDStream = accessLogDStream
   .transformToPair(LogAnalyzerStreamingTotalRefactored::ipAddressCount)
   .updateStateByKey(COMPUTE_RUNNING_SUM)
   .transform(LogAnalyzerStreamingTotalRefactored::filterIPAddress);
ipAddressesDStream.foreachRDD(rdd -> {
  List<String> ipAddresses = rdd.take(100);
  System.out.println("All IPAddresses > 10 times: " + ipAddresses);
  return null;
});
```

Take a closer look and run [LogAnalyzerStreamingTotalRefactored.java](java8/src/main/java/com/databricks/apps/logs/chapter1/LogAnalyzerStreamingTotalRefactored.java)
now.
