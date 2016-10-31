name := "BigDataComputeEvents"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.0.1"

libraryDependencies ++= Seq(
  "io.jvm.uuid" %% "scala-uuid" % "0.2.1",
  "org.scalaz" %% "scalaz-core" % "7.2.6",
  "org.scala-lang" % "scala-compiler" % "2.11.8",
  "org.scala-lang" % "scala-reflect" % "2.11.8",
  "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.4",
  "org.scala-lang.modules" % "scala-parser-combinators_2.11" % "1.0.4",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.0",
  "com.github.nscala-time" %% "nscala-time" % "2.14.0",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.0-M2"
)