# Chapter 2: Importing and Exporting Data

In the last chapter we covered how to get started with Spark for logs analysis,
but in those examples, we just pulled data in from a local file and printed
the statistics to standard out.  In this chapter, we go over techniques for
loading and exporting data that is suitable for a production system.  In particular, the techniques must scale to handle large production volumes of logs.

To scale, Apache Spark is meant to be deployed on a cluster of machines.
Read the [Spark Cluster Overview Guide](https://spark.apache.org/docs/latest/cluster-overview.html),
so that you understand the different between the Spark driver vs. the worker nodes.

While you
could continue running the examples in local mode, it is recommended
that you set up a Spark cluster to run the remaining examples on and get practice working with the cluster - such as familiarizing yourself with the web interface of the cluster.  You can run a small cluster on your local machine by following the instructions for  [Spark Standalone Mode](https://spark.apache.org/docs/latest/spark-standalone.html).  Optionally, if you have access to more machines - such as on AWS or your organization has its own datacenters, consult the cluster overview guide for information on how to set up a Spark cluster there.

Once you get a Spark cluster up:
* Use spark-submit to run your jobs rather than using the JVM parameter.  Run one of the
examples from the previous chapter to check your set up.
* Poke around and familiarize with the web interfaces for Spark.

Then, proceed on to the next sections:

* [Importing Data](import.md)
* [Exporting Data](export.md)
