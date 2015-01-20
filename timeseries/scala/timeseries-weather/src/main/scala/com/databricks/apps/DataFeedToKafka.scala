package com.databricks.apps

import scala.util.Try
import scala.concurrent.duration._
import scala.collection.immutable
import scala.io.BufferedSource
import akka.stream.FlowMaterializer
import akka.stream.scaladsl.Source
import akka.actor._
import akka.cluster.Cluster
import com.databricks.apps.weather.WeatherEvent
import com.databricks.apps.weather.WeatherSettings
import com.datastax.spark.connector.embedded.KafkaEvent.KafkaMessageEnvelope

/** Simulates real time weather events individually sent to Kafka.
  *
  * For security reasons, only one app (many instances) knows how to talk to Kafka instances in it's data center.
  * And that is the sample primary weather app which can read/write. This just sends to
  * the [[com.databricks.apps.weather.NodeGuardian]] which abstracts that pathway. */
class DataFeedActor(actor: ActorSelection, settings: WeatherSettings) extends Actor with ActorLogging {

  import WeatherEvent._
  import FileFeedEvent._
  import context.dispatcher

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
          context.actorOf(Props(new FileFeedActor(actor, settings))) ! message
        }
      case message =>
        context.system.scheduler.scheduleOnce(20.seconds) {
          context.actorOf(Props(new FileFeedActor(actor, settings))) ! message
        }
    }
  }

  def stop(): Unit = if (context.children.isEmpty) context stop self
}

class FileFeedActor(actor: ActorSelection, settings: WeatherSettings) extends Actor with ActorLogging {

  import WeatherEvent._
  import FileFeedEvent._
  import settings._
  import context.dispatcher

  implicit val materializer = FlowMaterializer()

  def receive : Actor.Receive = {
    case e: FileSource => handle(e, sender)
  }

  def handle(e : FileSource, origin : ActorRef): Unit = {
    val source = e.source
    log.info(s"Ingesting {}", e.file.getAbsolutePath)

    Source(source.getLines).foreach { case data =>
      context.system.scheduler.scheduleOnce(2.second) {
        log.debug(s"Sending '{}'", data)
        actor ! KafkaMessageEnvelope[String, String](KafkaTopicRaw, KafkaGroupId, data)
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

