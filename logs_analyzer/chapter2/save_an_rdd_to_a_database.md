# Save an RDD to a Database

You can write your own custom writer and call a transform on your RDD to write each element to a database of your choice, but there's a lot of ways to write something that looks like it would work, but does not work well in a distributed environment.  Here are some things to watch out for:

* A common naive mistake is to open a connection on the Spark driver program, and then try to use that connection on the Spark workers.  The connection should be opened on the Spark worker, such as by calling ```forEachPartition``` and opening the connection inside that function.
* Use partitioning to control the parallelism for writing to your data storage.  Your data storage may not support too many concurrent connections.
* Use batching for writing out multiple objects at a time if batching is optimal for your data storage.
* Make sure your write mechanism is resilient to failures.  Writing out a very large dataset can take a long time, which increases the chance something can go wrong - a network failure, etc.
* Consider utilizing a static pool of database connections on your Spark workers.
* If you are writing to a sharded data storage, partition your RDD to match your sharding strategy.  That way each of your Spark workers only connects to one database shard, rather than each Spark worker connecting to every database shard.

Be cautious when writing out so much data, and make sure you understand the distributed nature of Spark!
