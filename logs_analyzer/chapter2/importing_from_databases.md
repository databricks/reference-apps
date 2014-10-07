# Reading from Databases

Most likely, you aren't going to be storing your logs data in a database (that is likely too expensive), but there may be other data you want to input to Spark that is stored in a database.  Perhaps that data can be joined with the logs to provide more information.

The same way file systems have evolved over time to scale, so have databases.

A simple database to begin with is a single database - SQL databases are quite common.  When that fills, one option is to buy a larger machine for the database.  The price of these larger machines gets increasingly expensive (even price per unit of storage) and it is eventually no longer possible to buy a machine big enough at some point.  A common choice then is to switch to sharded databases.  With that option, application level code is written to determine on which database shard a piece of data should be read or written to.

To read data in from a SQL database, the JdbcRDD is one option for a moderate amount of data:
* https://spark.apache.org/docs/0.8.1/api/core/org/apache/spark/rdd/JdbcRDD.html

Recently, there has been a movement in the database world towards **NoSQL** or **Key-Value** databases that were designed to scale.  For these databases, it's usually transparent to the application developer that the underlying database stores data on multiple machines. **Cassandra** is one very popular NoSQL database.

To read data from Cassandra into Spark, see the Spark Cassandra Connector:
* https://github.com/datastax/spark-cassandra-connector

If you use a different database, Spark may have a built-in library for importing from that database, but more often 3rd parties offer Spark integration - so search for that. 

As usual, reading a small amount of data from a database is much easier than reading a ton of data.  It's important to understand your database and Spark's distributed programming model in order to write optimal code for importing a very large dataset.


