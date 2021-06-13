name := "Sprint2"

version := "0.1"

scalaVersion := "2.12.10"

val hadoopVersion = "2.7.3"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion

resolvers += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos"

libraryDependencies += "org.apache.hive" % "hive-jdbc" % "1.1.0-cdh5.16.2"


val circeVersion = "0.12.3"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

