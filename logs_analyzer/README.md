# Log Analysis with Spark

This project demonstrates how easy it is to do log analysis with Apache Spark.

Log analysis is an ideal use case for Spark.  It's a very large, common data source and contains a rich set of information.  Spark allows you to store your logs in files to disk cheaply, while still providing a
quick and simple way to process them.  We hope this project will show you how to use Apache Spark on your organization's production logs and fully harness the power of that data.  Log data can be used for monitoring your servers, improving business and customer intelligence, building recommendation systems, preventing fraud, and much more.

## How to use this project

This project is broken up into sections with bite-sized examples for
demonstrating new Spark functionality for log processing.  This makes
the examples easy to run and learn as they cover just one new topic at a time.
At the end, we assemble some of these examples to form a sample log analysis application.

###[Section 1: Introduction to Apache Spark](chapter1/README.md)

The Apache Spark library is introduced, as well as Spark SQL and Spark Streaming.  By the
end of this chapter, a reader will know how to call transformations and actions and work
with RDDs and DStreams.

###[Section 2: Importing Data](chapter2/README.md)

This section includes examples to illustrate how to get data into Spark and starts covering concepts of distributed computing.
The examples are all suitable for datasets that are too large to be processed on one
machine.

###[Section 3: Exporting Data](chapter3/README.md)

This section includes examples to illustrate how to get data out of Spark.  Again, concepts of a distributed
computing environment are reinforced, and the examples are suitable for large datasets.

### [Section 4: Logs Analyzer Application](app/README.md)

This section puts together some of the code in the other chapters to form
a sample log analysis application.

## More to come...

While that's all for now, there's definitely more to come over time.
