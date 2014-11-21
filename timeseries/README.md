# Time Series with Spark, Spark Streaming, Cassandra and Kafka

This project demonstrates how easy it is to do time series analysis with Apache Spark and Cassanrda.

There are many flavors of time series data. Some can be windowed in the stream, others can not be windowed in the stream
because queries are not by time slice but by specific year,month,day,hour. Spark Streaming lets you do both.
Cassandra in particular is excellent for time series data, working with raw data, transformations with Spark to aggregate data,
and so forth. In some cases, using Spark with Cassandra (and the right data model) reduces the number of Spark
transformations necessary on your data because Cassandra does that for you in its cluster.

Co-location of Spark and Cassandra nodes assists in data-locality, decreases network calls, and reduced latency.

## How to use this project

There is currently just one sample app with a simple client.

### Setup - 3 Steps
1. [Download the latest Cassandra](http://cassandra.apache.org/download/) and open the compressed file.

        Optional: open /apache-cassandra-{latest.version}/conf/cassandra.yaml and increase batch_size_warn_threshold_in_kb to 64

2. Start Cassandra - you may need to prepend with sudo, or chown /var/lib/cassandra. On the command line:


    ./apache-cassandra-{latest.version}/bin/cassandra -f

3. Run the setup cql scripts to create the schema and populate the weather stations table.

On the command line start a cqlsh shell:

    cd /path/to/reference-apps/timeseries/scala/data
    ~/apache-cassandra-{latest.version}/bin/cqlsh

You should see:

    Connected to Test Cluster at 127.0.0.1:9042.
    [cqlsh {latest.version} | Cassandra {latest.version} | CQL spec {latest.version} | Native protocol {latest.version}]
    Use HELP for help.
    cqlsh>

Run the script:

    cqlsh> source 'create-timeseries.cql';
    cqlsh> quit;

[Find out more] (https://github.com/killrweather/killrweather/wiki/2.-Code-and-Data-Setup#data-setup)

### Run the app and client app: runnable by command line or in an IDE
#### To Run from an IDE
First start com.databricks.apps.WeatherApp, then start com.databricks.apps.WeatherClientApp.

#### To Run from Command Line

    cd /path/to/reference-apps/timeseries/scala
    sbt weather/run

You should see:

    Multiple main classes detected, select one to run:
    [1] com.databricks.apps.WeatherApp
    [2] com.databricks.apps.WeatherClientApp

Select 1, then in a second window do the same and select 2.

### About The Time Series Data Model
[Find Out More](https://github.com/killrweather/killrweather/wiki/4.-Time-Series-Data-Model)