name := "SparkStreaming"

version := "0.1"
//
//scalaVersion := "2.12.12"

//libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % "2.4.6",
//  "org.apache.spark" %% "spark-sql" % "2.4.6",
//  "org.apache.spark" %% "spark-streaming" % "2.4.6"
//  "com.twitter" % "hbc-twitter4j" % "2.2.0",
//  "com.twitter" % "hbc-core" % "2.2.0"
//
//)

//val sparkVersion = "2.4.6"
//
//libraryDependencies ++= Seq(
//
//  "org.apache.spark" %% "spark-core" % sparkVersion,
//
//  "org.apache.spark" % "spark-streaming_2.12" % "2.4.6" % "provided",
//
//  "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0",
//
//  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
//
//  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
//
//  "org.apache.bahir" %% "spark-streaming-twitter" % "2.3.2"
//
////  "org.apache.spark" %% "spark-sql-kafka-0-10_2.11" % "2.4.0"
//
//)

scalaVersion := "2.12.11"

val sparkVersion = "2.4.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" % "spark-streaming_2.12" % sparkVersion,
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.12" % sparkVersion,
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0"
)
