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
  * First run the setup described in the README.
  *
  * For an IDE first start [[WeatherApp]], then start [[WeatherClientApp]].
  * By command line:
  *   {{{
  *     cd /path/to/reference-apps/timeseries/scala
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
