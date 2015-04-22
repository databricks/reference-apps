# Weather TimeSeries Data Application with Cassandra

This project demonstrates how to easily leverage and integrate Apache Spark, Spark Streaming,  Apache Cassandra with the Spark Cassandra Connector and Apache Kafka in general, and more specifically for time series data. It also demonstrates how to do this in an asynchronous Akka event-driven environment. We use weather data and the existing hourly data format as the sample domain.

## Time Series Data
The use of time series data for business analysis is not new. What is new is the ability to collect and analyze massive volumes of data in sequence at extremely high velocity to get the clearest picture to predict and forecast future market changes, user behavior, environmental conditions, resource consumption, health trends and much, much more.

Apache Cassandra is a NoSQL database platform particularly suited for these types of Big Data challenges. Cassandra’s data model is an excellent fit for handling data in sequence regardless of data type or size. When writing data to Cassandra, data is sorted and written sequentially to disk. When retrieving data by row key and then by range, you get a fast and efficient access pattern due to minimal disk seeks – time series data is an excellent fit for this type of pattern. Apache Cassandra allows businesses to identify meaningful characteristics in their time series data as fast as possible to make clear decisions about expected future outcomes.

There are many flavors of time series data. Some can be windowed in the stream, others can not be windowed in the stream because queries are not by time slice but by specific year,month,day,hour. Spark Streaming lets you do both. In some cases, such as in the KafkaStreamingActor, using Spark with Cassandra (and the right data model) reduces the number of Spark transformations necessary on your data because Cassandra does the work for you in its cluster.


