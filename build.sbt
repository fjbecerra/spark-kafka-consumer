name := "simple-consumer-spark"

version := "1.0"
scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.2" %  "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.6.2" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.2"
libraryDependencies += "com.typesafe" % "config" % "1.2.1"

