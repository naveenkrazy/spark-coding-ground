package com.codingground.sparkbatch.logprocessor

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkException}

object GenerateMetrics extends App {

  final val logger = Logger.getLogger(getClass)
  logger.setLevel(Level.INFO)

  logger.info("Creating new Spark Session with initial properties....")
  val conf : SparkConf = new SparkConf()
    .setMaster("local[*]").setAppName("Log_Analyzer")
    .set("spark.io.compression.codec", "snappy")
    .set("spark.driver.memory", "3g")

  val spark = SparkSession.builder.config(conf).getOrCreate

  val processor = DataProcessor(spark, args)

  try {
    processor.process()
  }
  catch {
    case s: SparkException =>
      logger.info("Spark exception occurred during the log data processing......")
      logger.error(s.getMessage)

    case e: Exception =>
      logger.error("Unknown exception occurred during the spark job execution...")
      logger.error(e.printStackTrace())
  }
  finally {
    logger.info("stopping spark session...")
    spark.stop()
  }

}
