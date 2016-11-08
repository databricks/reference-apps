# Chapter 4: Logs Analzyer Reference App in Java 8.

To compile this code, use maven:
```
% mvn package
```

To run the program, you can use spark-submit program:
```
%  ${YOUR_SPARK_HOME}/bin/spark-submit \
   --class "com.databricks.apps.logs.LogAnalyzerAppMain" \
   --master spark://YOUR_SPARK_MASTER \
   target/uber-log-analyzer-2.0.jar
```

Configure values for flags - see LogAnalyzerAppMain.

Drop new apache access log files into the specified logs directory.
Open the local html page in a browser and refresh it to see an 
update set of statistics from your log files.
