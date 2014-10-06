## Exporting Data out of Spark

This section contains methods for exporting data out of Spark into systems.  First, you'll have to figure out if your output data
is small (meaning can fit on memory on one machine) or large
(too big to fit into memory on one machine).  Consult these two sections
based on your use case.

* [Small Datasets](small.md) - If you have a small dataset, you can call an action on
this dataset to retrieve objects in memory on the driver program, and then write
those objects out any way you want.
* [Large Datasets](large.md) - For a large dataset, it's important to remember that this
dataset is too large to fit in memory on the driver program.  In that case, you
can either call Spark to write the data to files directly from the Spark workers or you can implement your own custom solution.
