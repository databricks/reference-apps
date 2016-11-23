name := "Spark Logs Analyzer"

version := "2.0"

scalaVersion := "2.11.8"

scalacOptions := Seq("-unchecked", "-deprecation")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.0.1" % "provided"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"