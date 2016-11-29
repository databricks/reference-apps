# First Logs Analyzer in Spark

Before beginning this section, go through [Spark Quick Start](https://spark.apache.org/docs/latest/quick-start.html)
and familiarize with the [Spark Programming Guide](https://spark.apache.org/docs/latest/programming-guide.html)
first.

This section requires a dependency on the Spark Core library in the maven file - note update this dependency based on the version of Spark you have installed:
```xml
<dependency> <!-- Spark -->
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-core_2.11</artifactId>
    <version>2.0.1</version>
</dependency>
```

Before we can begin, we need two things:

* **An Apache access log file**: If you have one, it's more interesting to use real
data.
  * This is trivial sample one provided at
 [data/apache.access.log](../data/apache.access.log).
  * Or download a better example here: http://www.monitorware.com/en/logsamples/apache.php
* **A parser and model for the log file**: See
 [ApacheAccessLog.java](java8/src/main/java/com/databricks/apps/logs/ApacheAccessLog.java).

The example code uses an Apache access log file since that's a well known
and common log format.  It would be easy to rewrite the parser for a different log format if you have data in another log format.

The following statistics will be computed:

* The average, min, and max content size of responses returned from the server.
* A count of response code's returned.
* All IPAddresses that have accessed this server more than N times.
* The top endpoints requested by count.

Let's first walk through the code first before running the example at [LogAnalyzer.java](java8/src/main/java/com/databricks/apps/logs/chapter1/LogAnalyzer.java).

The main body of a simple Spark application is below.
The first step is to bring up a Spark context.  Then the Spark context
can load data from a text file as an RDD, which it can then process.  Finally, before exiting the function, the Spark context is stopped.

```java
public class LogAnalyzer {
  public static void main(String[] args) {
    // Create a Spark Context.
    SparkConf conf = new SparkConf().setAppName("Log Analyzer");
    JavaSparkContext sc = new JavaSparkContext(conf);

    // Load the text file into Spark.
    if (args.length == 0) {
      System.out.println("Must specify an access logs file.");
      System.exit(-1);
    }
    String logFile = args[0];
    JavaRDD<String> logLines = sc.textFile(logFile);

    // TODO: Insert code here for processing logs.

    sc.stop();
  }
}
```

Given an RDD of log lines, use the ```map``` function to transform each line
to an ApacheAccessLog object.  The ApacheAccessLog RDD is cached in memory,
since multiple transformations and actions will be called on it.

```java
// Convert the text log lines to ApacheAccessLog objects and
// cache them since multiple transformations and actions
// will be called on the data.
JavaRDD<ApacheAccessLog> accessLogs =
    logLines.map(ApacheAccessLog::parseFromLogLine).cache();
```

It's useful to define a sum reducer - this is a function that takes in
two integers and returns their sum.  This is used all over our example.
```java
private static Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;
```

Next, let's calculate the average, minimum, and maximum content size of the
response returned.  A ```map``` transformation extracts the content sizes, and
then different actions (```reduce```, ```count```, ```min```, and ```max```) are called to output
various stats.  Again, call ```cache``` on the context size RDD to avoid recalculating those values for each action called on it.

```java
// Calculate statistics based on the content size.
// Note how the contentSizes are cached as well since multiple actions
//   are called on that RDD.
JavaRDD<Long> contentSizes =
   accessLogs.map(ApacheAccessLog::getContentSize).cache();
System.out.println(String.format("Content Size Avg: %s, Min: %s, Max: %s",
    contentSizes.reduce(SUM_REDUCER) / contentSizes.count(),
    contentSizes.min(Comparator.naturalOrder()),
    contentSizes.max(Comparator.naturalOrder())));
```

To compute the response code counts, we have to work with key-value pairs - by using ```mapToPair``` and ```reduceByKey```.
Notice that we call ```take(100)``` instead of ```collect()``` to gather the final output of the response code counts.
Use extreme caution before calling ```collect()``` on an RDD since all that data will be sent to a single Spark driver and can cause the driver to run out of memory.  Even in this case where there are only a limited number of response codes and it seems safe - if there are malformed lines in the Apache access log or a bug in the parser, there could be many invalid response codes to cause an.

```java
// Compute Response Code to Count.
List<Tuple2<Integer, Long>> responseCodeToCount = accessLogs
        .mapToPair(log -> new Tuple2<>(log.getResponseCode(), 1L))
        .reduceByKey(SUM_REDUCER)
        .take(100);
System.out.println(String.format("Response code counts: %s", responseCodeToCount));
```

To compute any IPAddress that has accessed this server more than 10 times,
we call the ```filter``` tranformation and then ```map``` to retrieve only the IPAddress and discard the count.  Again we use ```take(100)``` to retrieve the values.
```java
List<String> ipAddresses =
    accessLogs.mapToPair(log -> new Tuple2<>(log.getIpAddress(), 1L))
        .reduceByKey(SUM_REDUCER)
        .filter(tuple -> tuple._2() > 10)
        .map(Tuple2::_1)
        .take(100);
System.out.println(String.format("IPAddresses > 10 times: %s", ipAddresses));
```

Last, let's calculate the top endpoints requested in this log file. We define
an inner class, ```ValueComparator``` to help with that.  This function tells us,
given two tuples, which one is first in ordering.  The key of the tuple is ignored, and ordering is based just on the values.

```java
private static class ValueComparator<K, V>
   implements Comparator<Tuple2<K, V>>, Serializable {
  private Comparator<V> comparator;

  public ValueComparator(Comparator<V> comparator) {
    this.comparator = comparator;
  }

  @Override
  public int compare(Tuple2<K, V> o1, Tuple2<K, V> o2) {
    return comparator.compare(o1._2(), o2._2());
  }
}
```

Then, we can use the ```ValueComparator``` with the ```top``` action to compute the top endpoints accessed on this server according to how many times the endpoint was accessed.

```java
List<Tuple2<String, Long>> topEndpoints = accessLogs
    .mapToPair(log -> new Tuple2<>(log.getEndpoint(), 1L))
    .reduceByKey(SUM_REDUCER)
    .top(10, new ValueComparator<>(Comparator.<Long>naturalOrder()));
System.out.println("Top Endpoints: " + topEndpoints);
```

These code snippets are from [LogAnalyzer.java](java8/src/main/java/com/databricks/apps/logs/chapter1/LogAnalyzer.java).
Now that we've walked through the code, try running that example.  See the README for language specific instructions for building and running.
