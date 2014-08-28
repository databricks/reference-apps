# Chapter 1: Logs Analzyer Reference App in Java 8.

To compile this code, use maven:
```
% mvn package
```

To run the program, you can use spark-submit program:
```
%  ${YOUR_SPARK_HOME}/bin/spark-submit
   --class "com.databricks.apps.logs.LogsAnalyzerReferenceAppMain"
   --master spark://YOUR_SPARK_MASTER
   target/log-analyzer-1.0.jar
```

Change values in the Flags inner class of LogAnalyzerAppMain 

Drop new apache access log files into the specified logs directory.
Open the local html page in a browser and refresh it to see an 
update set of statistics from your log files.
