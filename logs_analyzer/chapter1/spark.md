# A First Log Analyzer in Spark

It would be helpful to go through the [Spark Quick Start](https://spark.apache.org/docs/latest/quick-start.html)
and skim the [Spark Programming Guide](https://spark.apache.org/docs/latest/programming-guide.html)
before beginning this section.

Before we start, we need two things:

* *An Apache access log file*: If you have one, it's more interesting to use real
data.  If not, you can use the sample one provided at
 [data/apache.access.log](../data/apache.access.log).
* *A parser and model for the log file* - see
 [ApacheAccessLog.java](java8/src/main/java/com/databricks/apps/logs/ApacheAccessLog.java).

The example code uses an Apache access log file since that's a well known
and common log format.  It would be easy to rewrite the parser for a different log format if needed.

The following statistics will be computed:

* The average, min, and max content size of responses returned from the server.
* A count of response code's returned.
* All IPAddresses that have accessed this server more than N times.
* The top endpoints requested by count.

Before we run anything, let's understand the code.

The main body of a simple Spark application is below.
The first step is to bring up a Spark context.  Then  the Spark context
can load data from a text file as an RDD, which can then process.  Finally, before exiting the function, we stop the Spark context.

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

First, we'll define a sum reducer - this is a function that takes in
two integers and returns their sum.  This is used all over our example.
```java
private static Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;
```

Next, let's calculate the average, minimum, and maximum content size of the
response returned.  A ```map``` transformation extracts the content sizes, and
then different actions (```reduce```, ```count```, ```min```, and ```max```) are called to output
various stats.  Again, call ```cache``` the context size RDD to avoid repeating computation.

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
That's just to be safe since it could take a really long time to call
```collect()``` on a very large dataset.

```java
// Compute Response Code to Count.
List<Tuple2<Integer, Long>> responseCodeToCount =
    accessLogs.mapToPair(log -> new Tuple2<>(log.getResponseCode(), 1L))
        .reduceByKey(SUM_REDUCER)
        .take(100);
System.out.println(String.format("Response code counts: %s", responseCodeToCount));
```

To compute any IPAddress that has accessed this server more than 10 times,
we call the ```filter``` tranformation and then ```map``` to retrieve only the IPAddress and not the count.  Again we use ```take(100)``` to retrieve the values.
```java
List<String> ipAddresses =
    accessLogs.mapToPair(log -> new Tuple2<>(log.getIpAddress(), 1L))
        .reduceByKey(SUM_REDUCER)
        .filter(tuple -> tuple._2() > 10)
        .map(Tuple2::_1)
        .take(100);
System.out.println(String.format("IPAddresses > 10 times: %s", ipAddresses));
```

Last let's calculate the top endpoints requested in this log file. We define
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
Now that we've walked through the code, try running that example.
