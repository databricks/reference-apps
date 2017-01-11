import re

from pyspark.sql import Row

# The last item, content size, could be 0, represented either as '0' or '-'
# For the log from  http://www.monitorware.com/en/logsamples/apache.php
# Still there is one line that does not conform. 
# To save time I just delete that line of log for this tutorial.

APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+|-)'

# Returns a dictionary containing the parts of the Apache Access Log.
def parse_apache_log_line(logline):
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
   if match is None:
        raise Error("Invalid logline: %s" % logline)
    else:
    	val = long(match.group(9)) if match.group(9)!='-' else 0
    return Row(
        ip_address    = match.group(1),
        client_identd = match.group(2),
        user_id       = match.group(3),
        date_time     = match.group(4),
        method        = match.group(5),
        endpoint      = match.group(6),
        protocol      = match.group(7),
        response_code = int(match.group(8)),
        content_size  = val
    )
