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
package com.databricks.apps.core

import scala.util.Try
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.spark.connector.cql.{NoAuthConf, PasswordAuthConf, AuthConf}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.Logging

/**
 * (Type safe) shared settings. First attempts to acquire from the runtime environment.
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
class Settings(conf: Option[Config] = None) extends Serializable with Logging {

  val rootConfig = conf match {
    case Some(c) => c.withFallback(ConfigFactory.load)
    case _ => ConfigFactory.load
  }

  protected val kafka = ConfigFactory.load.getConfig("kafka")
  //val KafkaHosts: immutable.Seq[String] = Util.immutableSeq(timeseries.getStringList("kafka.hosts"))
  val KafkaEncoderFqcn = kafka.getString("encoder.fqcn")
  val KafkaDecoderFqcn = kafka.getString("decoder.fqcn")
  val KafkaPartitioner = kafka.getString("partitioner.fqcn")
  val KafkaBatchSendSize = kafka.getInt("batch.send.size")

  protected val spark = rootConfig.getConfig("spark")
  protected val cassandra = rootConfig.getConfig("cassandra")

  val SparkMaster = withFallback[String](Try(spark.getString("master")),
    "spark.master") getOrElse "local[*]"

  val SparkCleanerTtl = withFallback[Int](Try(spark.getInt("cleaner.ttl")),
    "spark.cleaner.ttl") getOrElse (3600*2)

  val SparkStreamingBatchInterval = withFallback[Int](Try(spark.getInt("streaming.batch.interval")),
    "spark.streaming.batch.interval") getOrElse 1

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

  val CassandraRpcPort = withFallback[Int](Try(cassandra.getInt("connection.rpc.port")),
    "spark.cassandra.connection.rpc.port") getOrElse 9160

  val CassandraNativePort = withFallback[Int](Try(cassandra.getInt("connection.native.port")),
    "spark.cassandra.connection.native.port") getOrElse 9042

  /* Tuning */

  val CassandraKeepAlive = withFallback[Int](Try(cassandra.getInt("connection.keep-alive")),
    "spark.cassandra.connection.keep_alive_ms") getOrElse 1000

  val CassandraRetryCount = withFallback[Int](Try(cassandra.getInt("connection.query.retry-count")),
    "spark.cassandra.query.retry.count") getOrElse 10

  val CassandraConnectionReconnectDelayMin = withFallback[Int](Try(cassandra.getInt("connection.reconnect-delay.min")),
    "spark.cassandra.connection.reconnection_delay_ms.min") getOrElse 1000

  val CassandraConnectionReconnectDelayMax = withFallback[Int](Try(cassandra.getInt("reconnect-delay.max")),
    "spark.cassandra.connection.reconnection_delay_ms.max") getOrElse 60000

  /* Reads */
  val CassandraReadPageRowSize = withFallback[Int](Try(cassandra.getInt("read.page.row.size")),
    "spark.cassandra.input.page.row.size") getOrElse 1000

  val CassandraReadConsistencyLevel: ConsistencyLevel = ConsistencyLevel.valueOf(
    withFallback[String](Try(cassandra.getString("read.consistency.level")),
      "spark.cassandra.input.consistency.level") getOrElse ConsistencyLevel.LOCAL_ONE.name)

  val CassandraReadSplitSize = withFallback[Long](Try(cassandra.getLong("read.split.size")),
    "spark.cassandra.input.split.size") getOrElse 100000

  /* Writes */

  val CassandraWriteParallelismLevel = withFallback[Int](Try(cassandra.getInt("write.concurrent.writes")),
    "spark.cassandra.output.concurrent.writes") getOrElse 5

  val CassandraWriteBatchSizeBytes = withFallback[Int](Try(cassandra.getInt("write.batch.size.bytes")),
    "spark.cassandra.output.batch.size.bytes") getOrElse 64 * 1024

  private val CassandraWriteBatchSizeRows = withFallback[String](Try(cassandra.getString("write.batch.size.rows")),
    "spark.cassandra.output.batch.size.rows") getOrElse "auto"

  val CassandraWriteBatchRowSize: Option[Int] = {
    val NumberPattern = "([0-9]+)".r
    CassandraWriteBatchSizeRows match {
      case "auto"           => None
      case NumberPattern(x) => Some(x.toInt)
      case other =>
        throw new IllegalArgumentException(
          s"Invalid value for 'cassandra.output.batch.size.rows': $other. Number or 'auto' expected")
    }
  }

  val CassandraWriteConsistencyLevel: ConsistencyLevel = ConsistencyLevel.valueOf(
    withFallback[String](Try(cassandra.getString("write.consistency.level")),
      "spark.cassandra.output.consistency.level") getOrElse ConsistencyLevel.LOCAL_ONE.name)

  val CassandraDefaultMeasuredInsertsCount: Int = 128

  /** Attempts to acquire from environment, then java system properties. */
  private def withFallback[T](env: Try[T], key: String): Option[T] = env match {
    case null  => None
    case value => value.toOption
  }
}

