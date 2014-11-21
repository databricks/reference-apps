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

import java.util.Properties
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._
import akka.actor.{Cancellable, ActorLogging, Actor}
import kafka.consumer.{Consumer, ConsumerConfig}
import kafka.serializer.StringDecoder

class KafkaConsumer(zookeeper: String, topic: String, groupId: String, partitions: Int, numThreads: Int, count: AtomicInteger) {

  val connector = Consumer.create(createConsumerConfig)

  val streams = connector
    .createMessageStreams(Map(topic -> partitions), new StringDecoder(), new StringDecoder())
    .get(topic)

  val executor = Executors.newFixedThreadPool(numThreads)

  for(stream <- streams) {
    executor.submit(new Runnable() {
      def run() {
        for(s <- stream) {
          while(s.iterator.hasNext) {
            count.getAndIncrement
          }
        }
      }
    })
  }

  private def createConsumerConfig: ConsumerConfig = {
    val props = new Properties()
    props.put("consumer.timeout.ms", "2000")
    props.put("zookeeper.connect", zookeeper)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "10")
    props.put("auto.commit.interval.ms", "1000")

    new ConsumerConfig(props)
  }

  def shutdown() {
    println("Consumer shutting down.")
    Option(connector) map (_.shutdown())
    Option(executor) map (_.shutdown())
  }
}

/** Simple actor with a Kafka consumer to report the latest message count in a Kafka Topic. */
class KafkaTopicListener(zkConnect: String, topic: String, group: String, taskInterval: FiniteDuration = 1.seconds)
  extends Actor with ActorLogging {
  import Event._
  import context.dispatcher

  val atomic = new AtomicInteger(0)

  val task = context.system.scheduler.schedule(1.seconds, taskInterval) {
    self ! QueryTask
  }

  var consumer: Option[KafkaConsumer] = None

  override def preStart(): Unit =
    context.system.eventStream.subscribe(self, classOf[NodeInitialized])

  override def postStop(): Unit = {
    task.cancel()
    consumer map (_.shutdown())
    context.system.eventStream.unsubscribe(self)
  }

  def receive: Actor.Receive = {
    case NodeInitialized(_)           => start()
    case QueryTask if atomic.get >= 0 => report()
    case QueryTask => // ignore but don't drop to deadLetters
  }

  def report(): Unit = log.info("Kafka message count [{}]", atomic.get)

  def start(): Unit = {
    log.info("Starting Kafka topic reporting.")
    consumer = Some(new KafkaConsumer(zkConnect, topic, group, 1, 10, atomic))
  }
}