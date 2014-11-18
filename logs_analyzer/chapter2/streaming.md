# Streaming Data Import

This section covers importing data for streaming.  The streaming example in the
previous chapter received data through a single socket - which is not
a scalable solution.  In a real production system, there are many servers
continuously writing logs, and we want to process all of those files.  This
section contains scalable solutions for data import.  Since streaming is now
used, there is no longer the need for a nightly batch job to process logs,
but instead - this logs processing program can be long-lived - continuously
receiving new logs data, processing the data, and computing log stats.

1. [Built In Methods for Streaming Import](built_in.md)
* [Kafka](kafka.md)


