# Logs Analyzer in Spark

This project demonstrates how easy it is to do logs analysis with Apache Spark.

Logs analysis is an ideal use case for Spark.  It's a very large, common data source and contains a rich set of information.  Spark allows you to store your logs
in files to disk cheaply, while still providing a
quick and simple way to process them.  We hope this project will show you to use
Apache Spark on your organization's production logs and fully harness the power
of that data.  Logs data can be used for monitoring your servers, improving business and customer intelligence, building recommendation systems, preventing fraud, and much more.

This project will also appeal to those who want to learn Spark and
learn better by example.  Those readers can browse the chapters, see
what features of the logs analyzer is similar to their use case, and
refashion the code samples for their needs.

Additionally, this is meant to be a practical guide for using Spark in your
systems, so we'll even touch other technologies that are compatible with Spark - such as where to store your files.

## How to use this project

This project is broken up into chapters with bite-sized examples for
demonstrating new Spark functionality for logs processing.  This makes
the examples easy to run and learn as they cover just one new topic at a time.
At the end, we assemble some of these examples to form a sample logs
analyzer application.

###[Chapter 1: Introduction to Apache Spark](chapter1/README.md)

The Apache Spark library is introduced, including RDD's, transformation,
and actions.  We'll also introduce Spark SQL and Spark Streaming.  By the
end of this chapter, a reader will know how to do queries with Apache Spark.

###[Chapter 2: Importing and Exporting Data](chapter2/README.md)

This chapter includes examples to illustrate how to get your data in
and out of Spark.  It is also recommended that you bring up a local Spark cluster, as you should know how to run Spark on a dataset that is too
big to fit on one machine.

### [Final Chapter: Logs Analyzer Application](app/README.md)

This section puts together some of the code in the other chapters to form
a sample log analysis application.

## More to come...

While that's all for now, there's definitely more to come - expect more features to the log reference application over time.
