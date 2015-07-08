# Building the Twitter Classifier Assembly

Steps for building the assembly:

* Run SBT using the `assembly` target.  Example: `$ ./sbt/sbt clean assembly`

Upon successfully building the assembly, you should be able to run the various Spark jobs as documented in the [Gitbook](https://www.gitbook.io/read/book/databricks/databricks-spark-reference-applications).

The default setting is to use Spark 1.4.  You should able to run this coding with other versions of Spark 1.X and higher by changing the Spark version in the build file (assuming no API changes).
