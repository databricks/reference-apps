# Exporting Large Datasets

If you are exporting a very large dataset, you can't call ```collect()``` or
a similar action to read all the data from the RDD onto the single driver
program - that could trigger out of memory problems.  Instead, you have to be careful about saving a large RDD.  See these two sections for more information.

* [Save the RDD to Files](save_the_rdd_to_files.md) - There are built in methods in Spark for saving a large RDD to files.
* [Save the RDD to a Database](save_an_rdd_to_a_database.md) - This section contains recommended best practices for saving a large RDD to a database.
