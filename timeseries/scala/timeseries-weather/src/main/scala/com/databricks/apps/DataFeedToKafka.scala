package com.databricks.apps

import scala.util.Try
import scala.concurrent.duration._
import scala.io.BufferedSource
import akka.stream.FlowMaterializer
import akka.stream.scaladsl.Source
import akka.actor._
import akka.cluster.Cluster
import kafka.serializer.StringEncoder
import kafka.producer.ProducerConfig
import com.databricks.apps.weather.WeatherEvent
import com.databricks.apps.weather.WeatherSettings
import com.datastax.spark.connector.embedded._

/** Simulates real time weather events individually sent to Kafka. */
class DataFeedActor(settings: WeatherSettings) extends Actor with ActorLogging {

  import WeatherEvent._
  import FileFeedEvent._
  import context.dispatcher

  val producerConfig: ProducerConfig = {
    val props = new java.util.Properties()
    props.put("metadata.broker.list", "localhost:9092")
    props.put("serializer.class", classOf[StringEncoder].getName)
    props.put("partitioner.class", "kafka.producer.DefaultPartitioner")
    props.put("producer.type", "async")
    props.put("request.required.acks", "1")
    props.put("batch.num.messages", "100")
    new ProducerConfig(props)
  }

  def receive: Actor.Receive = {
   case e @ FileStreamEnvelope(toStream) => start(e)
    case TaskCompleted                   => stop()
  }

  /** Simulates a feed vs a burst. */
  def start(envelope: FileStreamEnvelope): Unit = {
    log.info("Simulating data ingestion on {}.", Cluster(context.system).selfAddress)

    envelope.files.map {
      case message if message == envelope.files.head =>
        context.system.scheduler.scheduleOnce(5.seconds) {
          context.actorOf(Props(new FileFeedActor(producerConfig, settings))) ! message
        }
      case message =>
        context.system.scheduler.scheduleOnce(20.seconds) {
          context.actorOf(Props(new FileFeedActor(producerConfig, settings))) ! message
        }
    }
  }

  def stop(): Unit = if (context.children.isEmpty) context stop self
}

/** [[KafkaProducerActor]] is a simple Kafka producer for an Akka Actor using string encoder and default partitioner.
  * The [[FileFeedActor]] receives file-based data (simulative a live API) receive of raw data,
  * pipes the data through Akka Streams to publish to Kafka and handle life cycle. */
class FileFeedActor(val producerConfig: ProducerConfig, settings: WeatherSettings) extends KafkaProducerActor[String, String] {

  import WeatherEvent._
  import FileFeedEvent._
  import KafkaEvent._
  import settings._
  import context.dispatcher

  implicit val materializer = FlowMaterializer()

  def feedReceive : Actor.Receive = {
    case e: FileSource => handle(e, sender)
  }

  override def receive : Actor.Receive = feedReceive orElse super.receive

  /** Simulates bursts vs a flood of data. */
  def handle(e : FileSource, origin : ActorRef): Unit = {
    val source = e.source
    log.info(s"Ingesting {}", e.file.getAbsolutePath)

    Source(source.getLines).foreach { case data =>
      context.system.scheduler.scheduleOnce(2.second) {
        log.debug(s"Sending '{}'", data)
        self ! KafkaMessageEnvelope[String, String](KafkaTopicRaw, KafkaGroupId, data)
      }
    }.onComplete { _ =>
      Try(source.close())
      origin ! TaskCompleted
      context stop self
    }
  }
}

private[apps] object FileFeedEvent {
  import java.io.{BufferedInputStream, FileInputStream, File => JFile}
  import java.util.zip.GZIPInputStream

  @SerialVersionUID(0L)
  sealed trait FileFeedEvent extends Serializable
  case class FileStreamEnvelope(files: Set[FileSource]) extends FileFeedEvent
  case class FileSource(file: JFile) extends FileFeedEvent {
    def source: BufferedSource = file match {
      case null =>
        throw new IllegalArgumentException("FileStream: File must not be null.")
      case f if f.getAbsolutePath endsWith ".gz" =>
        scala.io.Source.fromInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(file))), "utf-8")
      case f =>
        scala.io.Source.fromFile(file, "utf-8")
    }
  }
}

