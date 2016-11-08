# Spark SQL

You should go through the [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
before beginning this section.

This section requires an additioal dependency on Spark SQL:
```xml
<dependency> <!-- Spark SQL -->
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-sql_2.11</artifactId>
    <version>2.0.1</version>
</dependency>
```

For those of you who are familiar with SQL, the same statistics we calculated
in the previous example can be done using Spark SQL rather than calling
Spark transformations and actions directly.  We walk through how to do that
here.

First, we need to create a SparkSession instance - an entry point for every Spark SQL application.
Using SparkSession, we read a text file and obtain a Dataset of Strings,
where every string represents a line from the input file. Finally, we convert the
Dataset of Strings to Dataset of ApacheAccessLog entries.
To become familiar with Dataset concept, refer to
[Datasets and DataFrames](http://spark.apache.org/docs/latest/sql-programming-guide.html#datasets-and-dataframes)
```java
public class LogAnalyzerSQL {
  public static void main(String[] args) {
    // Initialize SparkSession instance.
    SparkSession sparkSession = SparkSession
            .builder()
            .appName("Log Analyzer SQL")
            .getOrCreate();
    JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

    if (args.length == 0) {
      System.out.println("Must specify an access logs file.");
      System.exit(-1);
    }
    String logFile = args[0];

    // Read Dataset of lines from the file.
    // Note how we convert Dataset of String lines to Dataset of ApacheAccessLog objects
    // using an Encoder.
    Dataset<ApacheAccessLog> accessLogs = sparkSession
            .read()
            .textFile(logFile)
            .map(ApacheAccessLog::parseFromLogLine,
                    Encoders.bean(ApacheAccessLog.class));

    // TODO: Insert code for computing log stats.

    sc.stop();
  }
}
```

Next, we register our logs data into a view in order to run SQL
queries on it.
```java
// Register the Dataset as a temporary view.
accessLogs.createOrReplaceTempView("logs");
```

Now, we are ready to start running some SQL queries on our view.  Here's
the code to compute the identical statistics in the previous section - it
should look very familiar for those of you who know SQL. For transformations
on Datasets, Spark SQL uses Encoders. The API provides Encoders for all
widely used datatypes, as well as utilities to create Encoders for composite
data types like tuples.
```java
// Calculate statistics based on the content size.
Row contentSizeStats = sparkSession
        .sql("SELECT SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize) FROM logs")
        .first();
System.out.println(String.format("Content Size Avg: %s, Min: %s, Max: %s",
    contentSizeStats.getLong(0) / contentSizeStats.getLong(1),
    contentSizeStats.getLong(2),
    contentSizeStats.getLong(3)));

// Compute Response Code to Count.
// Note the use of "LIMIT 1000" since the number of responseCodes
// can potentially be too large to fit in memory.
List<Tuple2<Integer, Long>> responseCodeToCount = sparkSession
    .sql("SELECT responseCode, COUNT(*) FROM logs GROUP BY responseCode LIMIT 100")
    .map(row -> new Tuple2<>(row.getInt(0), row.getLong(1)),
            Encoders.tuple(Encoders.INT(), Encoders.LONG()))
    .collectAsList();
System.out.println(String.format("Response code counts: %s", responseCodeToCount));

// Any IPAddress that has accessed the server more than 10 times.
List<String> ipAddresses = sparkSession
    .sql("SELECT ipAddress, COUNT(*) AS total FROM logs GROUP BY ipAddress HAVING total > 10 LIMIT 100")
    .map(row -> row.getString(0), Encoders.STRING())
    .collectAsList();
System.out.println(String.format("IPAddresses > 10 times: %s", ipAddresses));

// Top Endpoints.
List<Tuple2<String, Long>> topEndpoints = sparkSession
    .sql("SELECT endpoint, COUNT(*) AS total FROM logs GROUP BY endpoint ORDER BY total DESC LIMIT 10")
    .map(row -> new Tuple2<>(row.getString(0), row.getLong(1)),
            Encoders.tuple(Encoders.STRING(), Encoders.LONG()))
    .collectAsList();
System.out.println(String.format("Top Endpoints: %s", topEndpoints));
```

Note that the default SQL dialect does not allow using reserved keyworks as alias names.  In other words, ```SELECT COUNT(*) AS count``` will cause errors, but ```SELECT COUNT(*) AS the_count``` runs fine.  If you use the HiveQL parser though, then you should be able to use anything as an identifier.

Try running [LogAnalyzerSQL.java](java8/src/main/java/com/databricks/apps/logs/chapter1/LogAnalyzerSQL.java) now.
