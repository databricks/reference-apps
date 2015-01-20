/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

import sbt._
import sbt.Keys._

object ReferenceAppsBuild extends Build {
  import Settings._

  /** Will likely add another sample app. Weather being the first. */
  lazy val root = Project(
    id = "root",
    base = file("."),
    settings = parentSettings,
    aggregate = Seq(weather)
  )

  lazy val weather = Project(
    id = "weather",
    base = file("./timeseries-weather"),
    settings = defaultSettings ++ Seq(libraryDependencies ++= Dependencies.app)
  )

}

object Versions {

  val Akka           = "2.3.4"//2.3.8
  val AkkaStreams    = "0.11"
  val CassandraDriver = "2.1.3"
  val Chill          = "1.1"
  val JDK            = "1.7"
  val JodaConvert    = "1.7"
  val JodaTime       = "2.4"
  val Kafka          = "0.8.0"//0.8.2 when out of beta
  val Logback        = "1.0.0"
  val Scala          = "2.10.4"
  val Slf4j          = "1.7.7"
  val Spark          = "1.2.0"
  val SparkCassandra = "1.2.0-alpha1"
}

object Dependencies {
  import Versions._

  object Compile {

    val akkaStreams       = "com.typesafe.akka"   %% "akka-stream-experimental"           % AkkaStreams    // ApacheV2
    val akkaActor         = "com.typesafe.akka"   %% "akka-actor"                         % Akka           // ApacheV2
    val akkaCluster       = "com.typesafe.akka"   %% "akka-cluster"                       % Akka           // ApacheV2
    val akkaRemote        = "com.typesafe.akka"   %% "akka-remote"                        % Akka           // ApacheV2
    val akkaSlf4j         = "com.typesafe.akka"   %% "akka-slf4j"                         % Akka           // ApacheV2
    val driver            = "com.datastax.cassandra" % "cassandra-driver-core"            % CassandraDriver  exclude("com.google.guava", "guava") excludeAll(ExclusionRule("org.slf4j"))
    val jodaTime          = "joda-time"           % "joda-time"                           % JodaTime        // ApacheV2
    val jodaConvert       = "org.joda"            % "joda-convert"                        % JodaConvert     // ApacheV2
    val kafka             = "org.apache.kafka"    %% "kafka"                              % Kafka  excludeAll(ExclusionRule("org.slf4j")) // ApacheV2
    val kafkaStreaming    = "org.apache.spark"    %% "spark-streaming-kafka"              % Spark  exclude("com.google.guava", "guava") exclude("org.apache.spark", "spark-core") // ApacheV2
    val logback           = "ch.qos.logback"      % "logback-classic"                     % Logback
    val sparkCassandra    = "com.datastax.spark"  %% "spark-cassandra-connector"          % SparkCassandra  excludeAll(ExclusionRule("org.slf4j"))// ApacheV2
    val sparkCassandraEmb = "com.datastax.spark"  %% "spark-cassandra-connector-embedded" % SparkCassandra  excludeAll(ExclusionRule("org.slf4j"))// ApacheV2
  }

  import Compile._

  val akka = Seq(akkaActor, akkaCluster, akkaRemote, akkaSlf4j, akkaStreams)

  val connector = Seq(driver, sparkCassandra, sparkCassandraEmb)

  val core = connector ++ akka ++ Seq(kafka, jodaConvert, jodaTime)

  val app = core ++ Seq(kafkaStreaming, logback)

}

object Settings extends Build {
  import net.virtualvoid.sbt.graph.Plugin.graphSettings
  import com.scalapenos.sbt.prompt.SbtPrompt.autoImport._
  import com.scalapenos.sbt.prompt.PromptTheme

  lazy val buildSettings = Seq(
    name := "timeseries samples with cassandra and kafka",
    normalizedName := "timeseries",
    scalaVersion := Versions.Scala,
    homepage := Some(url("https://github.com/killrweather/killrweather")),
    promptTheme := theme
  )

  val parentSettings = buildSettings ++ Seq(
    publishArtifact := false,
    publish := {}
  )

  override lazy val settings = super.settings ++ buildSettings

  lazy val defaultSettings = graphSettings ++ Seq(
    autoCompilerPlugins := true,
    libraryDependencies <+= scalaVersion { v => compilerPlugin("org.scala-lang.plugins" % "continuations" % v) },
    scalacOptions ++= Seq("-encoding", "UTF-8", s"-target:jvm-${Versions.JDK}", "-feature", "-language:_", "-deprecation", "-unchecked", "-Xfatal-warnings", "-Xlint"),
    javacOptions in Compile ++= Seq("-encoding", "UTF-8", "-source", Versions.JDK, "-target", Versions.JDK, "-Xlint:deprecation", "-Xlint:unchecked"),
    ivyLoggingLevel in ThisBuild := UpdateLogging.Quiet,
    parallelExecution in ThisBuild := false,
    parallelExecution in Global := false
  )

  lazy val theme = PromptTheme(List(
    text("[SBT] ", fg(green)),
    userName(fg(000)),
    text("@", fg(000)),
    hostName(fg(000)),
    text(":", fg(000)),
    gitBranch(clean = fg(green), dirty = fg(20)),
    text(":", fg(000)),
    currentProject(fg(magenta)),
    text("> ", fg(000))
  ))
}