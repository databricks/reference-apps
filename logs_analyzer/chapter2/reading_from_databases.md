# Reading from Databases

Most likely, you aren't going to be reading your logs data in from a databases, but there may be other data you want to input to Spark that is stored in a database.  It may be helpful to join that data with your logs.
The same way file systems have evolved to scale as datasets get bigger, so have databases.

A simple database to begin with is a single SQL database. One way to get a bigger database is to buy a larger machine.  Eventually, it is no longer possible to buy a database big enough or the price of a larger machine goes up exponentially, then a common choice is to switch to sharded SQL databases.  Then, there is application level code to determine on which database shard data should be read or written too.

To read data in from a SQL database, the JdbcRDD is one option:
* https://spark.apache.org/docs/0.8.1/api/core/org/apache/spark/rdd/JdbcRDD.html

There are also many **NoSQL** or **Key-Value** databases that were designed to scale.  For these databases, it's mostly transparent to the application developer that the underlying database stores data on multiple machines. **Cassandra** is a very popular NoSQL database.

To read data from Cassandra into Spark, see the Spark Cassandra Connector.
* https://github.com/datastax/spark-cassandra-connector

Spark is also compatible with many databases for import and more may
be added soon - often offered by a 3rd party - we won't go over them all here - refer to the
[Apache Spark Examples Directory](https://github.com/apache/spark/tree/master/examples)
which contains more examples that may be useful to you.
