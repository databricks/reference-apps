# Built In Methods for Streaming Import

The StreamingContext has many built in methods for importing data to streaming.
```socketTextStream``` was introduced in the previous chapter, and ```textFileStream```
is introduced here.  The ```textFileStream``` method monitors any Hadoop-compatible filesystem directory for new
files and when it detects a new file - reads it into Spark Streaming.
Just replace the call to ```socketTextStream``` with ```textFileStream```,
and pass in the directory to monitor for log files.

```java
// This methods monitors a directory for new files
// to read in for streaming.
JavaDStream<String> logData = jssc.textFileStream(directory);
```

Try running [LogAnalyzerStreamingImportDirectory.java](java8/src/main/java/com/databricks/apps/logs/chapter2/LogAnalyzerStreamingImportDirectory.java)
by specifying a directory.   You'll also need to drop or copy some new log files
into that directory while the program is running to see the calculated values update.

There are more built-in input methods for streaming - check them out in the
reference API documents for the StreamingContext.
