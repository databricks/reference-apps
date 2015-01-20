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
package com.databricks.apps.weather

import java.io.{File => JFile}

import scala.util.Try
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.Logging
import com.databricks.apps.FileFeedEvent.FileSource
import com.datastax.spark.connector.cql.{NoAuthConf, PasswordAuthConf, AuthConf}

/**
 * (Type safe) application settings. First attempts to acquire from the runtime environment.
 * If a setting is not found, attempts to acquire from -D java system properties,
 * finally falling back to a default config.
 *
 * Settings in the environment such as: SPARK_HA_MASTER="spark://host1@port1,host2@port2..." are picked up first.
 *
 * Settings from the command line in -D will override settings in the deploy environment.
 * For example: sbt -Dspark.master="spark://host3@port3,host4@port4..."
 *
 * If you have not yet used Typesafe Config before, you can pass in overrides like so:
 *
 * {{{
 *     new Settings(ConfigFactory.parseString("""spark.master = "some.ip""""))
 * }}}
 *
 * Any of these can also be overriden by your own application.conf.
 *
 * @param conf Optional config for test
 */
final class WeatherSettings(conf: Option[Config] = None) extends Serializable with Logging {

  val rootConfig = conf match {
    case Some(c) => c.withFallback(ConfigFactory.load)
    case _ => ConfigFactory.load
  }

  val AkkaBasePort = rootConfig.getInt("akka.remote.netty.tcp.port")

  protected val spark = rootConfig.getConfig("spark")

  val SparkMaster = withFallback[String](Try(spark.getString("master")),
    "spark.master") getOrElse "local[*]"

  val SparkCleanerTtl = withFallback[Int](Try(spark.getInt("cleaner.ttl")),
    "spark.cleaner.ttl") getOrElse (3600*2)

  val SparkStreamingBatchInterval = withFallback[Int](Try(spark.getInt("streaming.batch.interval")),
    "spark.streaming.batch.interval") getOrElse 1

  protected val cassandra = rootConfig.getConfig("cassandra")

  val CassandraHosts = withFallback[String](Try(cassandra.getString("connection.host")),
    "spark.cassandra.connection.host") getOrElse "127.0.0.1"

  logInfo(s"Starting up with spark master '$SparkMaster' cassandra hosts '$CassandraHosts'")

  val CassandraAuthUsername: Option[String] = Try(cassandra.getString("auth.username")).toOption
    .orElse(sys.props.get("spark.cassandra.auth.username"))

  val CassandraAuthPassword: Option[String] = Try(cassandra.getString("auth.password")).toOption
    .orElse(sys.props.get("spark.cassandra.auth.password"))

  val CassandraAuth: AuthConf = {
    val credentials = for (
      username <- CassandraAuthUsername;
      password <- CassandraAuthPassword
    ) yield (username, password)

    credentials match {
      case Some((user, password)) => PasswordAuthConf(user, password)
      case None                   => NoAuthConf
    }
  }

  val CassandraNativePort = withFallback[Int](Try(cassandra.getInt("connection.native.port")),
    "spark.cassandra.connection.native.port") getOrElse 9042

  /* App Specific Settings */
  protected val weather = rootConfig.getConfig("weather")

  val AppName = weather.getString("app-name")

  val SparkCheckpointDir = weather.getString("spark.checkpoint.dir")

  val CassandraKeyspace = weather.getString("cassandra.keyspace")
  val CassandraTableRaw = weather.getString("cassandra.table.raw")
  val CassandraTableDailyTemp = weather.getString("cassandra.table.daily.temperature")
  val CassandraTableDailyPrecip = weather.getString("cassandra.table.daily.precipitation")
  val CassandraTableCumulativePrecip = weather.getString("cassandra.table.cumulative.precipitation")
  val CassandraTableSky = weather.getString("cassandra.table.sky")
  val CassandraTableStations = weather.getString("cassandra.table.stations")

  protected val kafka = ConfigFactory.load.getConfig("kafka")
  val KafkaEncoderFqcn = kafka.getString("encoder.fqcn")
  val KafkaDecoderFqcn = kafka.getString("decoder.fqcn")
  val KafkaPartitioner = kafka.getString("partitioner.fqcn")
  val KafkaBatchSendSize = kafka.getInt("batch.send.size")
  val KafkaGroupId = kafka.getString("group.id")
  val KafkaTopicRaw = kafka.getString("topic.raw")

  val DataLoadPath = weather.getString("data.path")
  val IngestionData: Set[JFile] = {
    import java.io.{File => JFile}

    val files = new JFile(DataLoadPath).listFiles.collect {
      case file => file
    }.toSet

    log.info(s"Found ${files.size} data files to load.")
    require(files.nonEmpty, s"Unable to find valid data files at $DataLoadPath")
    files
  }

  val RawDataSources: Set[FileSource] = for (file <- IngestionData) yield FileSource(file)

  /** Attempts to acquire from environment, then java system properties. */
  private def withFallback[T](env: Try[T], key: String): Option[T] = env match {
    case null  => None
    case value => value.toOption
  }

}
