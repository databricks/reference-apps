# To run this code:
#
# % spark-submit \
#    --master local[4] \
#    --py-files databricks/apps/logs/apache_access_log.py \
#    databricks/apps/logs/log_analyzer.py \
#    ../../data/apache.accesslog
from pyspark import SparkContext, SparkConf

import apache_access_log
import sys

conf = SparkConf().setAppName("Log Analyzer")
sc = SparkContext(conf=conf)

logFile = sys.argv[1]

access_logs = (sc.textFile(logFile)
               .map(apache_access_log.parse_apache_log_line)
               .cache())

# Calculate statistics based on the content size.
content_sizes = access_logs.map(lambda log: log.content_size).cache()
print "Content Size Avg: %i, Min: %i, Max: %s" % (
    content_sizes.reduce(lambda a, b : a + b) / content_sizes.count(),
    content_sizes.min(),
    content_sizes.max()
    )

# Response Code to Count
responseCodeToCount = (access_logs.map(lambda log: (log.response_code, 1))
                       .reduceByKey(lambda a, b : a + b)
                       .take(100))
print "Response Code Counts: %s" % (responseCodeToCount)

# Any IPAddress that has accessed the server more than 10 times.
ipAddresses = (access_logs
               .map(lambda log: (log.ip_address, 1))
               .reduceByKey(lambda a, b : a + b)
               .filter(lambda s: s[1] > 10)
               .map(lambda s: s[0])
               .take(100))
print "IpAddresses that have accessed more then 10 times: %s" % (ipAddresses)

# Top Endpoints
topEndpoints = (access_logs
                .map(lambda log: (log.endpoint, 1))
                .reduceByKey(lambda a, b : a + b)
                .takeOrdered(10, lambda s: -1 * s[1]))
print "Top Endpoints: %s" % (topEndpoints)
