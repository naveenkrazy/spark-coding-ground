package com.secureworks.codingchallenge


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkFiles}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.Try



object GenerateMetrics {

  private val logger = Logger.getLogger(getClass)
  logger.setLevel(Level.DEBUG)

  def main(args: Array[String]): Unit = {


    logger.info("Extracting parameters from the console Args.....")
    val parameters = if(args.length >= 2) {
      extractParameters(args, Some(List("--class")))
    } else {
      logger.info("no parameters were passed from the command line")
      Map.empty[String, Any]
    }


    logger.info("Creating new Spark Session with initial properties....")
    val conf : SparkConf = new SparkConf()
      .setMaster("Local[*]").setAppName("Log_Analyzer")
      .set("spark.io.compression.codec", "snappy")

    val spark = SparkSession.builder.config(conf).getOrCreate

    val sc = spark.sparkContext

    logger.info("Adding source file to spark context to be to download on all executor nodes...")
    val sourceUrl = parameters.getOrElse("sourceUrl","ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz").toString

    sc.addFile(sourceUrl)

    val localPath = SparkFiles.get(sourceUrl.split("/").last)


    val srcRdd = sc.textFile(localPath).map(s => {
      val patternMap = Map(
        "hostPattern" -> """(\S+\.[\S+\.]+\S+)""".r,
        "tsPattern " -> """\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]""".r,
        "requestPattern" -> """\"(\S+)\s(\S+)\s*(\S*)\"""".r,
        "httpCodePattern " -> """\s(\d{3})\s""".r,
        "responseByteSizePattern" -> """\s(\d+)$""".r
      )
      val matchedObjects = patternMap.map(p => p._1 -> p._2.findFirstIn(s).getOrElse(""))
      val modifiedObjects = matchedObjects.map(t =>
        if(t._1 == "tsPattern") {
          t._2 match {
            case "" => t._2.trim
            case _ => getFormattedDate(t._2.toString).trim
          }
        } else {t._2.trim}
      )
      Row.fromSeq(modifiedObjects.toList)
    })

    val baseSchema = StructType(
      List(
        StructField("requestDate", StringType, true),
        StructField("visitor", StringType, true),
        StructField("totalBytes", StringType, true),
        StructField("responseCode", StringType, true),
        StructField("requestUrl", StringType, true)
      )
    )

    val base_df = spark.createDataFrame(srcRdd, baseSchema)

  }

  def extractParameters(args: Array[String], requiredParameters: Option[List[String]] = None): Map[String, Any] = {

    val parameters = args.sliding(2,1).toList.foldLeft(Map[String, Any]())((newMap, param) => {
      param match {
        case Array(name: String, value: String) =>
          if(name.startsWith("--")) {
            newMap + (name.substring(2) -> (if(value == "true" || value == "false") value.toBoolean else value) )
          } else {
            newMap
          }
      }
    })
    parameters
  }

  def getFormattedDate(dateString: String, srcFormat : Option[String] = None, tgtFormat: Option[String] = None) : String = {
    val cleanedDate = dateString.replaceAll("\\[", "")
      .replaceAll("\\]", "")
      .split(" ")(0)
    val sourceFormat = DateTimeFormat.forPattern(srcFormat.getOrElse("dd/MMM/yyyy:HH:mm:ss"))
    val targetFormat = DateTimeFormat.forPattern(tgtFormat.getOrElse("yyyy-MM-dd"))
    val parseDate = DateTime.parse(cleanedDate, sourceFormat)
    parseDate.toString(targetFormat)
  }

}
