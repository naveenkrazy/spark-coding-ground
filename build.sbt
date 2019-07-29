name := "spark-metrics"

version := "0.1"

scalaVersion := "2.11.11"

// also add ScalaTest as a framework to run the tests
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % Test

// Add the ScalaMock library (versions 4.0.0 onwards)
libraryDependencies += "org.scalamock" %% "scalamock" % "4.1.0" % Test

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.3"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.3"

libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.4.3"


