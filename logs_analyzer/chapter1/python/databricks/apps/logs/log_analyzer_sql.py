# To run this code:
#
# % spark-submit databricks/apps/logs/log_analyzer_sql.py
#    --py-files databricks/apps/logs/apache_access_log.py
#    ../../data/apache.access.log
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

import apache_access_log
import sys

conf = SparkConf().setAppName("Log Analyzer").setMaster("local[4]")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

logFile = sys.argv[1]

# TODO: Better parsing...
access_logs = (sc.textFile(logFile)
               .map(apache_access_log.parse_apache_log_line)
               .cache())

schema_access_logs = sqlContext.inferSchema(access_logs)
schema_access_logs.registerAsTable("logs")

# Calculate statistics based on the content size.
content_size_stats = (sqlContext
                      .sql("SELECT %s, %s, %s, %s FROM logs" % (
                           "SUM(contentSize) as theSum",
                           "COUNT(*) as theCount",
                           "MIN(contentSize) as theMin",
                           "MAX(contentSize) as theMax"))
                      .first())
print "Content Size Avg: %i, Min: %i, Max: %s" % (
    content_size_stats["theSum"] / content_size_stats["theCount"],
    content_size_stats["theMin"],
    content_size_stats["theMax"]
)

# Response Code to Count
responseCodeToCount = (sqlContext
                       .sql("SELECT responseCode, COUNT(*) AS theCount FROM logs GROUP BY responseCode")
                       .map(lambda row: (row['responseCode'], row['theCount']))
                       .take(100))
print "Response Code Counts: %s" % (responseCodeToCount)

# Any IPAddress that has accessed the server more than 10 times.
ipAddresses = (sqlContext
               .sql("SELECT ipAddress, COUNT(*) AS total FROM logs GROUP BY ipAddress HAVING total > 10")
               .map(lambda row: row['ipAddress'])
               .take(100))
print "All IPAddresses > 10 times: %s" % ipAddresses

# Top Endpoints
topEndpoints = (sqlContext
                .sql("SELECT endpoint, COUNT(*) AS total FROM logs GROUP BY endpoint ORDER BY total DESC LIMIT 10")
                .map(lambda row: (row['endpoint'], row['total']))
                .collect())
print "Top Endpoints: %s" % (topEndpoints)
