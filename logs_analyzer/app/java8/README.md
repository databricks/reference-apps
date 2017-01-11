# Chapter 4: Logs Analyzer Reference App in Java 8.

To compile this code, use maven:
```
% mvn package
```

To run the program, you can use spark-submit utility:
```
%  ${YOUR_SPARK_HOME}/bin/spark-submit \
   --class "com.databricks.apps.logs.LogAnalyzerAppMain" \
   --master spark://YOUR_SPARK_MASTER \
   target/uber-log-analyzer-2.0.jar \
   --logs-directory /tmp/logs \
   --output-html-file /tmp/log_stats.html \
   --window-length 30 \
   --slide-interval 5 \
   --checkpoint-directory /tmp/log-analyzer-streaming
```

For command line arguments description, see LogAnalyzerAppMain class documentation
or use `--help` command line argument:
```
%  ${YOUR_SPARK_HOME}/bin/spark-submit \
   --class "com.databricks.apps.logs.LogAnalyzerAppMain" \
   --master spark://YOUR_SPARK_MASTER \
   target/uber-log-analyzer-2.0.jar \
   --help
```

Drop new apache access log files into the directory specified with `--logs-directory` argument.
Open the HTML page specified with `--output-html-file` in your browser and see the statistics
collected from your log files.
