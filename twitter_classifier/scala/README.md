# Building the Twitter Classifier Assembly

Steps for building the assembly:

1. If you don't have [SBT](http://www.scala-sbt.org/) installed, follow the instructions on the SBT website.  The remaining instructions, below, assume you are using SBT 0.13. 
* Add the [`sbt-assembly`](https://github.com/sbt/sbt-assembly) plugin (SBT 0.13).  To do so globally, follow these steps:
  1. Create a SBT plugin directory.  Example: `$ mkdir -p ~/.sbt/0.13/plugins`
  * Add the published sbt-assembly plugin.  Example: `$ cat 'addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.11.2")' > ~/.sbt/0.13/plugins/assembly.sbt`
* Finally run SBT using the `assembly` target.  Example: `$ sbt clean assembly`

Upon successfully building the assembly, you should be able to run the various Spark jobs as documented in the [Gitbook](https://www.gitbook.io/read/book/databricks/databricks-spark-reference-applications).
