# Reusing Code from Batching: transform()

As you may have noticed, while the functions you called on a DStream
are named the same as those you called on an RDD in the batch example,
they are not the same methods, and it may not be clear how to reuse the code from the batch examples.  In
this section, we refactor the code from the batch examples and show how
to reuse it here.

DStreams have `transform` functions which allows you to call
any arbitrary RDD to RDD functions to RDD's in the DStream.  The
`transform` functions are perfect for reusing any RDD to RDD functions
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

The responseCodeCountDStream can be created by calling `transformToPair` with the `responseCodeCount` function to the accessLogDStream.
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
});
```

Take a closer look at [LogAnalyzerStreamingTotalRefactored.java](java8/src/main/java/com/databricks/apps/logs/chapter1/LogAnalyzerStreamingTotalRefactored.java)
now to see how that code has been refactored to reuse code from the batch example.
