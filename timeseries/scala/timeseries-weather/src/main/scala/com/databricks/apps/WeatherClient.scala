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

import scala.collection.immutable
import akka.actor._
import akka.cluster.Cluster
import com.typesafe.config.ConfigFactory
import com.databricks.apps.FileFeedEvent.FileStreamEnvelope
import com.databricks.apps.weather.{WeatherApiQueries, NodeGuardian, WeatherSettings}

/**
 * Exercises the weather app by triggering computations every 2 seconds via
 * the [[WeatherApiQueries]].
 */
object WeatherClient extends App {

  val settings = new WeatherSettings()
  import settings._

  /** Creates the client's ActorSystem on another port. */
  val system = ActorSystem(AppName,
    ConfigFactory.parseString(s"akka.remote.netty.tcp.port = 2551").withFallback(rootConfig))

  val cluster = Cluster(system)
  cluster.joinSeedNodes(seedNodes = immutable.Seq(cluster.selfAddress))

  /** This represents a separately-deployed process that would typically be feeding data to Kafka.
    * To simplify this as a runnable sample, we create this here so one does not have to
    * start up 3 processes in sbt: the main app, a data feed and the query client. */
  system.actorOf(Props(new DataFeedActor(settings)), "data-feed") ! FileStreamEnvelope(RawDataSources)

  val guardian = system.actorSelection(cluster.selfAddress.copy(port = Some(AkkaBasePort)) + "/user/node-guardian")

  /** Drives demo activity by sending requests to the [[NodeGuardian]] actor. */
  val queryClient = system.actorOf(Props(new WeatherApiQueries(settings, guardian)), "api-client")

}
