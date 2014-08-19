package com.databricks.apps.logs.chapter1;

import com.databricks.apps.logs.ApacheAccessLog;
import com.databricks.apps.logs.Functions;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import scala.Tuple2;
import scala.Tuple4;

import java.util.List;

/**
 * LogAnalyzerSQL shows how to use SQL syntax with Spark.
 *
 * Example command to run:
 * %  ${YOUR_SPARK_HOME}/bin/spark-submit
 *     --class "com.databricks.apps.logs.chapter1.LogsAnalyzerSQL"
 *     --master local[4]
 *     target/log-analyzer-1.0.jar
 *     ../../data/apache.access.log
 */
public class LogAnalyzerSQL {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Log Analyzer SQL");
    JavaSparkContext sc = new JavaSparkContext(conf);

    if (args.length == 0) {
      System.out.println("Must specify an access logs file.");
      System.exit(-1);
    }
    String logFile = args[0];
    JavaRDD<ApacheAccessLog> accessLogs = sc.textFile(logFile)
        .map(Functions.PARSE_LOG_LINE);

    JavaSQLContext sqlContext = new JavaSQLContext(sc);
    JavaSchemaRDD schemaRDD = sqlContext.applySchema(accessLogs,
        ApacheAccessLog.class).cache();
    schemaRDD.registerAsTable("logs");

    // Calculate statistics based on the content size.
    Tuple4<Long, Long, Long, Long> contentSizeStats =
        sqlContext.sql("SELECT SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize) FROM logs")
            .map(new Function<Row, Tuple4<Long, Long, Long, Long>>() {
              @Override
              public Tuple4<Long, Long, Long, Long> call(Row row)
                  throws Exception {
                return new Tuple4<Long, Long, Long, Long>(row.getLong(0),
                    row.getLong(1), row.getLong(2), row.getLong(3));
              }
            })
            .first();
    System.out.println(String.format("Content Size Avg: %s, Min: %s, Max: %s",
        contentSizeStats._1() / contentSizeStats._2(),
        contentSizeStats._3(),
        contentSizeStats._4()));

    // Compute Response Code to Count.
    List<Tuple2<Integer, Long>> responseCodeToCount = sqlContext
        .sql("SELECT responseCode, COUNT(*) FROM logs GROUP BY responseCode")
        .mapToPair(new PairFunction<Row, Integer, Long>() {
          @Override
          public Tuple2<Integer, Long> call(Row row) throws Exception {
            return new Tuple2<Integer, Long>(row.getInt(0), row.getLong(1));
          }
        })
        .take(1000);
    System.out.println(
        String.format("Response code counts: %s", responseCodeToCount));

    // Any IPAddress that has accessed the server more than 10 times.
    List<String> ipAddresses = sqlContext
        .sql("SELECT ipAddress, COUNT(*) AS total FROM logs GROUP BY ipAddress HAVING total > 10")
        .map(new Function<Row, String>() {
          @Override
          public String call(Row row) throws Exception {
            return row.getString(0);
          }
        })
        .take(100);  // Take only 100 in case this is a super large data set.
    System.out.println(
        String.format("IPAddresses > 10 times: %s", ipAddresses));

    // Top Endpoints.
    List<Tuple2<String, Long>> topEndpoints = sqlContext
        .sql("SELECT endpoint, COUNT(*) AS total FROM logs GROUP BY endpoint ORDER BY total DESC LIMIT 10")
        .map(new Function<Row, Tuple2<String, Long>>() {
               @Override
               public Tuple2<String, Long> call(Row row) throws Exception {
                 return new Tuple2<String, Long>(
                     row.getString(0), row.getLong(1));
               }
             })
        .collect();
    System.out.println(String.format("Top Endpoints: %s", topEndpoints));

    sc.stop();
  }
}
