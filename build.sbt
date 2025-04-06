name := "NonFlattenQueryEnabler"

version := "1.0"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.1"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.1.1"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.3.2" % Test,
  "org.scalatest" %% "scalatest" % "3.2.16" % Test
)



//libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % "3.1.1",
//  "org.apache.spark" %% "spark-sql" % "3.1.1",
//  "org.apache.spark" %% "spark-streaming" % "3.1.1",
//  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.1.1",
//  "org.apache.kafka" % "kafka-clients" % "2.8.1" // Ensure this version is compatible
//)


//libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.1"
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.1"
//libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.1.1"
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.1.1"
//libraryDependencies += "org.xerial" % "sqlite-jdbc" % "3.36.0.3"
//// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
//libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.1.1" % Test

