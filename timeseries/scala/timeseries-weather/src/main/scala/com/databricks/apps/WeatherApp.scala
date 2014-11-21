/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.databricks.apps

import akka.actor.{Props, ActorSystem, PoisonPill}
import com.databricks.apps.weather.{NodeGuardian, WeatherSettings}
import com.datastax.spark.connector.embedded._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/** Runnable.
  * I. 3-Step Setup:
  * 1. Download the latest Cassandra from http://cassandra.apache.org/download/ and open the compressed file
  *    Optional: open /apache-cassandra-{latest.version}/conf/cassandra.yaml and increase batch_size_warn_threshold_in_kb to 64
  *
  * 2. Start Cassandra - you may need to prepend with sudo, or chown /var/lib/cassandra
  *    On the command line:
  *   {{{
  *     ./apache-cassandra-{latest.version}/bin/cassandra -f
  *   }}}
  *
  * 3. Run the 2 setup cql scripts to create the schema and populate the weather stations table:
  *    On the command line start a cqlsh shell:
  *   {{{
  *     ./apache-cassandra-{latest.version}/bin/cqlsh
  *   }}}
  *
  * You should see:
  * Connected to Test Cluster at 127.0.0.1:9042.
  *    [cqlsh {latest.version} | Cassandra {latest.version} | CQL spec {latest.version} | Native protocol {latest.version}]
  *    Use HELP for help.
  *    cqlsh>
  *
  * Run the scripts.
  *   {{{
  *     cqlsh> source '/path/to/reference-apps/cassandra_kafka/scala/data/create-timeseries.cql';
  *     cqlsh> source '/path/to/reference-apps/cassandra_kafka/scala/data/load-timeseries.cql';
  *   }}}
  *
  * II. Run the app and client app: runnable by command line or in an IDE.
  * - For an IDE first start [[WeatherApp]], then start [[WeatherClientApp]].
  * - By command line:
  *   {{{
  *     cd /path/to/reference-apps/cassandra_kafka/scala
  *     sbt weather/run
  *   }}}
  *   You should see: Multiple main classes detected, select one to run:
  *   [1] com.databricks.apps.WeatherApp
  *   [2] com.databricks.apps.WeatherClientApp
  *
  * Select 1, then in a second window do the same and select 2.
  */
object WeatherApp extends App {

  val settings = new WeatherSettings
  import settings._

  /** Starts the Kafka broker and Zookeeper. */
  val kafka = new EmbeddedKafka

  /** Creates the raw data topic. */
  kafka.createTopic(KafkaTopicRaw)

  /** Configures Spark. */
  lazy val conf = new SparkConf().setAppName(getClass.getSimpleName)
    .setMaster(SparkMaster)
    .set("spark.cassandra.connection.host", CassandraHosts)
    .set("spark.serializer", classOf[KryoSerializer].getName)
    .set("spark.kryo.registrator", "com.databricks.apps.weather.WeatherKryoRegistrator")
    .set("spark.cleaner.ttl", SparkCleanerTtl.toString)

  lazy val sc = new SparkContext(conf)

  /** Creates the Spark Streaming context. */
  lazy val ssc = new StreamingContext(sc, Milliseconds(500))
 
  /** Creates the ActorSystem. */
  val system = ActorSystem(AppName, rootConfig)

  /* The root supervisor and traffic controller of the app. All inbound messages go through this actor. */
  val guardian = system.actorOf(Props(new NodeGuardian(ssc, kafka, settings)), "node-guardian")

  /** Registers the shutdown sequence. */
  system.registerOnTermination {
    kafka.shutdown()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
    guardian ! PoisonPill
  }
}
