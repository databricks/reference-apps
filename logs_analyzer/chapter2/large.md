# Exporting Large Datasets

If you are exporting a very large dataset, you can't call ```collect()``` or
a similar action to read all the data from the RDD onto the single driver
program.  Instead, call one of the save methods on the RDD
to write the data to files directly from the Spark workers.

## Save the RDD to files

RDD's have some built in methods for saving them to disk.  Once in files, many of the Hadoop databases can bulk load in data directly from files, as long as they are in
a specific format.  It's also worth mentioning [Sqoop](http://http://sqoop.apache.org/) - a very useful tool that can import Hadoop files into various databases.

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

Run [LogAnalyzerExportRDD.java](java8/src/main/java/com/databricks/apps/logs/chapter2/LogAnalyzerExportRDD.java) now.

Refer to the API documentation for other built in methods for saving to file.
There are different built in methods for saving RDD's to files in various formats, so skim the whole RDD package to see if there is something to suit your needs.

## Saving RDD directly to a data storage

Of course, you could also write your own custom writer and call a transform on your RDD to write each element to your data storage, but there's a lot of ways to write something that looks like it would work, but does not work well in a distributed environment.  Here are some things to watch out for:

* Use partitioning to control the parallelism for writing to your data storage.  Your data storage may not support too many concurrent connections.
* Use batching for writing out multiple objects at a time if batching is optimal for your data storage.
* Make sure your write mechanism is resilient to failures.  Writing out a very large dataset can take a long time, which increases the chance something can go wrong - a network failure, etc.
* A common naive mistake is to open a connection on the Spark driver program, and then try to use that connection on the Spark workers.  The connection should be opened on the Spark worker, such as by calling ```forEachPartition``` and opening the connection inside that function.
* Consider utilizing a static pool of database connections on your Spark workers.
* If you are writing to a sharded data storage, partition your RDD to match your sharding strategy.  That way each of your Spark workers only connects to one database shard, rather than each Spark worker connecting to every database shard.

Be cautious when writing out so much data!
