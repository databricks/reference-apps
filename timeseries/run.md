
# Running the Application

There are many flavors of time series data. Some can be windowed in the stream, others can not be windowed in the stream
because queries are not by time slice but by specific year,month,day,hour. Spark Streaming lets you do both.
Cassandra in particular is excellent for time series data, working with raw data, transformations with Spark to aggregate data,
and so forth. In some cases, using Spark with Cassandra (and the right data model) reduces the number of Spark
transformations necessary on your data because Cassandra does that for you in its cluster.

When using Apache Spark & Apache Cassandra together, it is best practice to co-locate Spark and Cassandra nodes for data-locality and decreased network calls, resulting in overall reduced latency.

## Setup
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

## Running the WeatherApp and WeatherClientApp

### To Run from an IDE
1. Start com.databricks.apps.WeatherApp
2. Then start com.databricks.apps.WeatherClientApp

### To Run from Command Line
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

### About The Time Series Data Model
[See this github repo to find out more about the Time Series Data Model](https://github.com/killrweather/killrweather/wiki/4.-Time-Series-Data-Model)
