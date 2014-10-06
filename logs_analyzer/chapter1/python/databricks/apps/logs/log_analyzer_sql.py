# To run this code:
#
# % spark-submit \
#    --py-files databricks/apps/logs/apache_access_log.py \
#    --master local[4] \
#    databricks/apps/logs/log_analyzer_sql.py \
#    ../../data/apache.accesslog
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

import apache_access_log
import sys

conf = SparkConf().setAppName("Log Analyzer")
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
                           "SUM(content_size) as theSum",
                           "COUNT(*) as theCount",
                           "MIN(content_size) as theMin",
                           "MAX(content_size) as theMax"))
                      .first())
print "Content Size Avg: %i, Min: %i, Max: %s" % (
    content_size_stats[0] / content_size_stats[1],
    content_size_stats[2],
    content_size_stats[3]
)

# Response Code to Count
responseCodeToCount = (sqlContext
                       .sql("SELECT response_code, COUNT(*) AS theCount FROM logs GROUP BY response_code LIMIT 100")
                       .map(lambda row: (row[0], row[1]))
                       .collect())
print "Response Code Counts: %s" % (responseCodeToCount)

# Any IPAddress that has accessed the server more than 10 times.
ipAddresses = (sqlContext
               .sql("SELECT ip_address, COUNT(*) AS total FROM logs GROUP BY ip_address HAVING total > 10 LIMIT 100")
               .map(lambda row: row[0])
               .collect())
print "All IPAddresses > 10 times: %s" % ipAddresses

# Top Endpoints
topEndpoints = (sqlContext
                .sql("SELECT endpoint, COUNT(*) AS total FROM logs GROUP BY endpoint ORDER BY total DESC LIMIT 10")
                .map(lambda row: (row[0], row[1]))
                .collect())
print "Top Endpoints: %s" % (topEndpoints)
