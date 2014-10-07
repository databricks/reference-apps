name := "Spark Logs Analyzer"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.1" % provided

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.0.1" % provided

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.0.1" % provided

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

