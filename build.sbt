name := "spark-metrics"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.4" % Test,
  "com.holdenkarau" %% "spark-testing-base" % "2.4.0_0.12.0" % Test,
  "org.apache.spark" % "spark-hive_2.11" % "2.4.3",
  "org.apache.spark" % "spark-core_2.11" % "2.4.3",
  "org.scalamock" %% "scalamock" % "4.1.0" % Test

)