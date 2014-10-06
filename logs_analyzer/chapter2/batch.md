# Batch Data Import

This section covers batch importing data into Apache Spark, such as
seen in the non-streaming examples from Chapter 1.  Those examples load data
from files all at once into one RDD, processes that RDD, the job completes,
and the program exits.  In a production system, you could set up a cron job to
kick off a batch job each night to process the last day's worth of log files and then publish statistics for the last day.

* [Importing From Files](importing_from_files.md) covers caveats when importing data from files.
* [Importing from Databases](importing_from_databases.md) links to examples of reading data from databases.


