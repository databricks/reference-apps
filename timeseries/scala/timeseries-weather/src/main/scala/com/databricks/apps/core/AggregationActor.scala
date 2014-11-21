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

import java.util.concurrent.TimeoutException

import akka.actor.SupervisorStrategy._
import akka.actor._
import akka.util.Timeout
import org.apache.spark.SparkException
import org.joda.time.{DateTime, DateTimeZone}

import scala.concurrent.duration._

/** A base actor for weather data computation. */
trait AggregationActor extends Actor {

  implicit val timeout = Timeout(5.seconds)

  implicit val ctx = context.dispatcher

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case _: SparkException           => Stop
      case _: ActorInitializationException => Stop
      case _: IllegalArgumentException => Stop
      case _: IllegalStateException    => Restart
      case _: TimeoutException         => Escalate
      case _: Exception                => Escalate
    }

  /** Creates a timestamp for the current date time in UTC. */
  def timestamp: DateTime = new DateTime(DateTimeZone.UTC)

}
