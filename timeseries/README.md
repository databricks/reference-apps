# Weather TimeSeries Data Application with Cassandra

This project demonstrates how to easily leverage and integrate Apache Spark, Spark Streaming,  Apache Cassandra with the Spark Cassandra Connector and Apache Kafka in general, and more specifically for time series data. It also demonstrates how to do this in an asynchronous Akka event-driven environment. We use weather data and the existing hourly data format as the sample domain.

## Time Series Data
The use of time series data for business analysis is not new. What is new is the ability to collect and analyze massive volumes of data in sequence at extremely high velocity to get the clearest picture to predict and forecast future market changes, user behavior, environmental conditions, resource consumption, health trends and much, much more.

Apache Cassandra is a NoSQL database platform particularly suited for these types of Big Data challenges. Cassandra’s data model is an excellent fit for handling data in sequence regardless of data type or size. When writing data to Cassandra, data is sorted and written sequentially to disk. When retrieving data by row key and then by range, you get a fast and efficient access pattern due to minimal disk seeks – time series data is an excellent fit for this type of pattern. Apache Cassandra allows businesses to identify meaningful characteristics in their time series data as fast as possible to make clear decisions about expected future outcomes.

There are many flavors of time series data. Some can be windowed in the stream, others can not be windowed in the stream because queries are not by time slice but by specific year,month,day,hour. Spark Streaming lets you do both. In some cases, such as in the KafkaStreamingActor, using Spark with Cassandra (and the right data model) reduces the number of Spark transformations necessary on your data because Cassandra does the work for you in its cluster.

## The Code
### WeatherApp

This is the primary compute application that processes the raw hourly data from the Kafka stream. As soon as each hourly raw data is received on the particular Kafka topic, it is mapped to a custom case class. From there, two initial actions are done:

*  The raw hourly data by weather station ID is stored in Cassandra for use by any other processes that want it in its raw state.
* The daily precipitation aggregate (0 to 23 hours of data) is computed and updated in a daily_aggregate_precipitation table in Cassandra. We can do precipitation in the stream because of the data model created and a Cassandra Counter used. Whereas aggregation of temperature require more input data so this is done by request with a station ID, year, month and day.
*
With things like daily precipitation and temperature aggregates pre-calculated and stored, any requests for annual aggregates, high-low or topK for example, can be easily computed via calls to specific Akka Actors handling each aggregation type (precip, temp, etc).  These read the aggregated and persisted data from Cassandra and runs the computation through Spark. Data is returned to the requester in a Future - no threads are blocked.
[Note, when in master with the correct URL of specific files available I can link them above so that a user can click on a specific block of code]

### WeatherClient
* Represents a separately-deployed process that would typically be feeding data to Kafka and other applications or processes that would be sending data aggregation requests.
* Simulates real-time raw weather events received from a separate process which this sends to Kafka.  It does this as a feed vs bursts on startup. The FileFeedActor in the client receives data per file, parses it to each raw data entry, and pipes the data through Akka Streams to publish to Kafka and handle life cycle.
* Exercises the weather app by sending requests for various aggregated data (topk, high-low, annual…) every n-seconds via the WeatherApiQueries Actor.



## Running the Application

There are many flavors of time series data. Some can be windowed in the stream, others can not be windowed in the stream
because queries are not by time slice but by specific year,month,day,hour. Spark Streaming lets you do both.
Cassandra in particular is excellent for time series data, working with raw data, transformations with Spark to aggregate data,
and so forth. In some cases, using Spark with Cassandra (and the right data model) reduces the number of Spark
transformations necessary on your data because Cassandra does that for you in its cluster.

When using Apache Spark & Apache Cassandra together, it is best practice to co-locate Spark and Cassandra nodes for data-locality and decreased network calls, resulting in overall reduced latency.

### Setup
1. [Download and install the latest Cassandra release](http://cassandra.apache.org/download/)

    * **Configuration Step:** Modify apache-cassandra-{latest.version}/conf/cassandra.yaml to increase batch_size_warn_threshold_in_kb to 64.
2. Start Cassandra.
```
    ./apache-cassandra-{latest.version}/bin/cassandra -f
```
**Note:** If you get an error - you may need to prepend with sudo, or chown /var/lib/cassandra.

3. Run the setup cql scripts to create the schema and populate the weather stations table.
Go to the timeseries data folder and start a cqlsh sell there:
```
    % cd /path/to/reference-apps/timeseries/scala/data
    % /path/to/apache-cassandra-{latest.version}/bin/cqlsh
```
You should see:
```
    Connected to Test Cluster at 127.0.0.1:9042.
    [cqlsh {latest.version} | Cassandra {latest.version} | CQL spec {latest.version} | Native protocol {latest.version}]
    Use HELP for help.
    cqlsh>
```
Then run the script:

    cqlsh> source 'create-timeseries.cql';
    cqlsh> quit;

[See this Github repo to find out more about the weather stations table data. ](https://github.com/killrweather/killrweather/wiki/2.-Code-and-Data-Setup#data-setup)

### Running the WeatherApp and WeatherClientApp

#### To Run from an IDE
1. Start com.databricks.apps.WeatherApp
2. Then start com.databricks.apps.WeatherClientApp

#### To Run from Command Line
1. Use SBT to run the app.
```
    % cd /path/to/reference-apps/timeseries/scala
    % sbt weather/run
```
You should see:
```
    Multiple main classes detected, select one to run:
    [1] com.databricks.apps.WeatherApp
    [2] com.databricks.apps.WeatherClientApp
```
Select option 1 to open the weather app.
2. Use SBT to run the client app.
Run the same commands above, but select option 2.

#### About The Time Series Data Model
[See this github repo to find out more about the Time Series Data Model](https://github.com/killrweather/killrweather/wiki/4.-Time-Series-Data-Model)
