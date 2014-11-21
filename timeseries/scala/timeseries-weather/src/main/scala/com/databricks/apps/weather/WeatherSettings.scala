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

import com.databricks.apps.core.Settings
import com.typesafe.config.Config

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
final class WeatherSettings(conf: Option[Config] = None) extends Settings(conf) {

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

  val KafkaGroupId = kafka.getString("group.id")
  val KafkaTopicRaw = kafka.getString("topic.raw")

  val DataLoadPath = weather.getString("data.load.path")
  val DataFileExtension = weather.getString("data.file.extension")
  val IngestionData: Set[String] = {
    import java.io.{File => JFile}

    val files = new JFile(s"$DataLoadPath").listFiles.collect {
      case file if file.exists => file.getAbsolutePath.replace("./", "")
    }.toSet

    log.info(s"Found ${files.size} data files to load.")
    require(files.nonEmpty, s"Unable to find valid data files at $DataLoadPath")
    files
  }
}
