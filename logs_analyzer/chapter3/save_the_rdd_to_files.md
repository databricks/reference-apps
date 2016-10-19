# Save the RDD to files

RDD's have some built in methods for saving them to disk.  Once in files, many of the Hadoop databases can bulk load in data directly from files, as long as they are in
a specific format.

In the following code example, we demonstrate the simple ```.saveAsTextFile()``` method.
This will write the data to simple text files where the ```.toString()``` method is called on each RDD element and one element is written per line.  The number of files output is equal to the
the number of partitions of the RDD being saved.  In this sample, the RDD is repartitioned to control the number of output files.

```java
public class LogAnalyzerExportRDD {
  // Optionally modify this based as makes sense for your dataset.
  public static final int NUM_PARTITIONS = 2;

  public static void main(String[] args) throws IOException {
    // Create the spark context.
    SparkConf conf = new SparkConf().setAppName("Log Analyzer SQL");
    JavaSparkContext sc = new JavaSparkContext(conf);

    if (args.length < 2) {
      System.out.println("Must specify an access logs file and an output file.");
      System.exit(-1);
    }
    String inputFile = args[0];
    String outputDirectory = args[1];
    JavaRDD<ApacheAccessLog> accessLogs = sc.textFile(inputFile)
        .map(ApacheAccessLog::parseFromLogLine)
        .repartition(NUM_PARTITIONS); // Optionally, change this.

    accessLogs.saveAsTextFile(outputDirectory);

    sc.stop();
  }
}
```

Run [LogAnalyzerExportRDD.java](java8/src/main/java/com/databricks/apps/logs/chapter2/LogAnalyzerExportRDD.java) now.  Notice that the number of files output is the same as the number of partitionds of the RDD.

Refer to the API documentation for other built in methods for saving to file.
There are different built in methods for saving RDD's to files in various formats, so skim the whole RDD package to see if there is something to suit your needs.

[Sqoop](http://sqoop.apache.org/) is a very useful tool that can import Hadoop files into various databases, and is thus very useful to use for getting the data written into files from Spark into your production database.
