name := "NonFlattenQueryEnabler"

version := "1.0"

scalaVersion := "2.12.10"

// Spark SQL
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.1"

// ScalaTest (compatible with Scala 2.12 and Spark 3.1.1)
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.3" % Test
