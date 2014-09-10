import AssemblyKeys._

name := "spark-twitter-lang-classifier"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.1.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.1.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.1.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.1.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.1.0"

libraryDependencies += "com.google.code.gson" % "gson" % "2.3"

libraryDependencies += "org.twitter4j" % "twitter4j-core" % "3.0.3"

libraryDependencies += "commons-cli" % "commons-cli" % "1.2"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

assemblySettings

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}
