# Exporting Small Datasets

If the data you are exporting out of Spark is small, you can just use an
action to convert the RDD into objects in memory on the driver program, and then write that output directly to any data storage solution of your
choosing.  You may remember that we called the ```take(N)``` action where N
is some finite number instead of the ```collect()``` action to ensure the output
fits in memory - no matter how big the input data set may be - this is good
practice.  This section walks through example code where you'll write the
log statistics to a file.

It may not be that useful to have these stats output to a file - in practice, you might write these statistics to a database for your presentation layer to access.

```java
LogStatistics logStatistics = logAnalyzerRDD.processRdd(accessLogs);

String outputFile = args[1];
Writer out = new BufferedWriter(
    new OutputStreamWriter(new FileOutputStream(outputFile)));

Tuple4<Long, Long, Long, Long> contentSizeStats =
    logStatistics.getContentSizeStats();
out.write(String.format("Content Size Avg: %s, Min: %s, Max: %s\n",
    contentSizeStats._1() / contentSizeStats._2(),
    contentSizeStats._3(),
    contentSizeStats._4()));

List<Tuple2<Integer, Long>> responseCodeToCount =
    logStatistics.getResponseCodeToCount();
out.write(String.format("Response code counts: %s\n", responseCodeToCount));

List<String> ipAddresses = logStatistics.getIpAddresses();
out.write(String.format("IPAddresses > 10 times: %s\n", ipAddresses));

List<Tuple2<String, Long>> topEndpoints = logStatistics.getTopEndpoints();
out.write(String.format("Top Endpoints: %s\n", topEndpoints));

out.close();
```

Now, run [LogAnalyzerExportSmallData.java](java8/src/main/java/com/databricks/apps/logs/chapter2/LogAnalyzerExportSmallData.java).  Try modifying it to write to a database of your own choosing.

