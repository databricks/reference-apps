# Spark SQL

You should go through the [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
before beginning this section.

This section requires an additioal dependency on Spark SQL:
```xml
<dependency> <!-- Spark SQL -->
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-sql_2.10</artifactId>
    <version>1.1.0</version>
</dependency>
```

For those of you who are familiar with SQL, the same statistics we calculated
in the previous example can be done using Spark SQL rather than calling
Spark transformations and actions directly.  We walk through how to do that
here.

First, we need to create a SQL Spark context. Note how we create one Spark
Context, and then use that to instantiate different flavors of Spark contexts.
You should not initialize multiple Spark contexts from the SparkConf in one process.
```java
public class LogAnalyzerSQL {
  public static void main(String[] args) {
    // Create the spark context.
    SparkConf conf = new SparkConf().setAppName("Log Analyzer SQL");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaSQLContext sqlContext = new JavaSQLContext(sc);

    if (args.length == 0) {
      System.out.println("Must specify an access logs file.");
      System.exit(-1);
    }
    String logFile = args[0];
    JavaRDD<ApacheAccessLog> accessLogs = sc.textFile(logFile)
        .map(ApacheAccessLog::parseFromLogLine);

    // TODO: Insert code for computing log stats.

    sc.stop();
  }
}
```

Next, we need a way to register our logs data into a table.  In Java, Spark SQL
can infer the table schema on a standard Java POJO - with getters and setters
as we've done with [ApacheAccessLog.java](java8/src/main/java/com/databricks/apps/logs/ApacheAccessLog.java).
(Note: if you are using a different language besides Java, there is a different
way for Spark to infer the table schema.  The examples in this directory work out of the
box.  Or you can also refer to the
[Spark SQL Guide on Data Sources](https://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources)
for more details.)
```java
JavaSchemaRDD schemaRDD = sqlContext.applySchema(accessLogs,
    ApacheAccessLog.class);
schemaRDD.registerTempTable("logs");
sqlContext.sqlContext().cacheTable("logs");
```

Now, we are ready to start running some SQL queries on our table.  Here's
the code to compute the identical statistics in the previous section - it
should look very familiar for those of you who know SQL:
```java
// Calculate statistics based on the content size.
Tuple4<Long, Long, Long, Long> contentSizeStats =
    sqlContext.sql("SELECT SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize) FROM logs")
        .map(row -> new Tuple4<>(row.getLong(0), row.getLong(1), row.getLong(2), row.getLong(3)))
        .first();
System.out.println(String.format("Content Size Avg: %s, Min: %s, Max: %s",
    contentSizeStats._1() / contentSizeStats._2(),
    contentSizeStats._3(),
    contentSizeStats._4()));

// Compute Response Code to Count.
// Note the use of "LIMIT 1000" since the number of responseCodes
// can potentially be too large to fit in memory.
List<Tuple2<Integer, Long>> responseCodeToCount = sqlContext
    .sql("SELECT responseCode, COUNT(*) FROM logs GROUP BY responseCode LIMIT 1000")
    .mapToPair(row -> new Tuple2<>(row.getInt(0), row.getLong(1)));
System.out.println(String.format("Response code counts: %s", responseCodeToCount))
    .collect();

// Any IPAddress that has accessed the server more than 10 times.
List<String> ipAddresses = sqlContext
    .sql("SELECT ipAddress, COUNT(*) AS total FROM logs GROUP BY ipAddress HAVING total > 10 LIMIT 100")
    .map(row -> row.getString(0))
    .collect();
System.out.println(String.format("IPAddresses > 10 times: %s", ipAddresses));

// Top Endpoints.
List<Tuple2<String, Long>> topEndpoints = sqlContext
    .sql("SELECT endpoint, COUNT(*) AS total FROM logs GROUP BY endpoint ORDER BY total DESC LIMIT 10")
    .map(row -> new Tuple2<>(row.getString(0), row.getLong(1)))
    .collect();
System.out.println(String.format("Top Endpoints: %s", topEndpoints));
```

Note that the default SQL dialect does not allow using reserved keyworks as alias names.  In other words, ```SELECT COUNT(*) AS count``` will cause errors, but ```SELECT COUNT(*) AS the_count``` runs fine.  If you use the HiveQL parser though, then you should be able to use anything as an identifier.

Try running [LogAnalyzerSQL.java](java8/src/main/java/com/databricks/apps/logs/chapter1/LogAnalyzerSQL.java) now.
