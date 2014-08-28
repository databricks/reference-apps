import re

APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)'

# Returns a dictionary containing the parts of the Apache Access Log.
def parse_apache_log_line(logline):
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        raise Error("Invalid logline: %s" % logline)
    return {
        "ipAddress"    : match.group(1),
        "clientIdentd" : match.group(2),
        "userId"       : match.group(3),
        "dateTime"      : match.group(4),
        "method"        : match.group(5),
        "endpoint"      : match.group(6),
        "protocol"      : match.group(7),
        "responseCode" : int(match.group(8)),
        "contentSize"  : long(match.group(9))
        }
