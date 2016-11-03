name := "spark-twitter-lang-classifier"

version := "2.0.0"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val sparkVer = "2.0.1"
  Seq(
    "org.apache.spark"     %% "spark-core"              % sparkVer withSources(),
    "org.apache.spark"     %% "spark-mllib"             % sparkVer withSources(),
    "org.apache.spark"     %% "spark-sql"               % sparkVer withSources(),
    "org.apache.spark"     %% "spark-streaming"         % sparkVer withSources(),
    "org.apache.spark"     %% "spark-streaming-twitter" % "1.6.2"  withSources(),
    "com.google.code.gson" %  "gson"                    % "2.8.0"  withSources(),
    "org.twitter4j"        %  "twitter4j-core"          % "4.0.5"  withSources(),
    "commons-cli"          % "commons-cli"              % "1.3.1"  withSources()
  )
}

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", "streaming", "twitter", _*) => MergeStrategy.deduplicate
  case PathList("org", "apache", "spark", _*)              => MergeStrategy.discard
  case PathList("org", "spark_project", _*)                => MergeStrategy.discard
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}
