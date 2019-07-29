package com.secureworks.codingchallenge


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, rank, regexp_extract}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkFiles}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.Try



object GenerateMetrics extends App {

  private val logger = Logger.getLogger(getClass)
  logger.setLevel(Level.INFO)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    logger.info("Extracting parameters from the console Args.....")
    val parameters = if(args.length >= 2) {
      extractParameters(args, Some(List("--class")))
    } else {
      logger.info("no parameters were passed from the command line")
      Map.empty[String, Any]
    }

    logger.info("Creating new Spark Session with initial properties....")
    val conf : SparkConf = new SparkConf()
      .setMaster("local[*]").setAppName("Log_Analyzer")
      .set("spark.io.compression.codec", "snappy")

    val spark = SparkSession.builder.config(conf).getOrCreate

    val sc = spark.sparkContext

    logger.info("Adding source file to spark context to be to download on all executor nodes...")
    val sourceUrl = parameters.getOrElse("sourceUrl","ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz").toString

    sc.addFile(sourceUrl)

    val localPath = SparkFiles.get(sourceUrl.split("/").last)

    logger.info("importing spark implicits for internal conversions....")
    import spark.implicits._

    logger.info("creating source rdd with flattening methods from ftp url....")
    val srcRdd = sc.textFile(localPath)
      .filter(!_.isEmpty)
      .map(s => {
        val patternMap = Map(
          "hostPattern" -> """(\S+\.[\S+\.]+\S+)""".r,
          "tsPattern" -> """\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]""".r,
          "requestPattern" -> """\"(\S+)\s(\S+)\s*(\S*)\"""".r,
          "httpCodePattern" -> """\s(\d{3})\s""".r,
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

    srcRdd.repartition(50).persist(StorageLevel.MEMORY_AND_DISK)

    logger.info(s"Total records from the source rdd created on GZip file from ftpSource: ${srcRdd.count.toString} ...")

    val baseSchema = StructType(
      List(
        StructField("requestDate", StringType, true),
        StructField("visitor", StringType, true),
        StructField("totalBytes", StringType, true),
        StructField("responseCode", StringType, true),
        StructField("requestUrl", StringType, true)
      )
    )

    logger.info("creating dataFrame from rdd to derive the metrics...")
    val src_df = spark.createDataFrame(srcRdd, baseSchema)

    src_df.show(20, false)

    val base_df = src_df.select(col("visitor"), col("requestDate"),
      regexp_extract($"requestUrl", """\"(\S+)\s(\S+)\s*(\S*)\"""", 1).alias("method"),
      regexp_extract($"requestUrl", """\"(\S+)\s(\S+)\s*(\S*)\"""", 2).alias("endpoint"),
      regexp_extract($"requestUrl", """\"(\S+)\s(\S+)\s*(\S*)\"""", 3).alias("protocol"),
      col("responseCode").cast("integer").alias("response"))
      .filter($"visitor" =!= ("") || $"response".isNotNull)

  srcRdd.unpersist()

  base_df.persist(StorageLevel.MEMORY_AND_DISK)

  logger.info(s"Total count for data frame after parsing all the data: ${base_df.count.toString}")

  base_df.show(20, false)

    val topVisitsByAddress = getTopVisitsFromDF(base_df, 10, Some("visitor"))
    val topVisitsByUrl = getTopVisitsFromDF(base_df, 10, Some("endPoint"))


    logger.info("showing top visits by Visitors.....")
    topVisitsByAddress.show(30, false)

    logger.info("showing top visits by Url.....")
    topVisitsByUrl.show(30, false)

    spark.catalog.clearCache()

    spark.stop()




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

  def getTopVisitsFromDF(inputDF: DataFrame, topN: Int, frequencyColumn: Option[String] = None): DataFrame = {
    val freqColumn = frequencyColumn.getOrElse("visitor")
    val freq_visitors = inputDF.groupBy("requestDate", s"$freqColumn").count()
      .select($"requestDate", $"$freqColumn", $"count".alias("freq_count"))

    val visitorWindow = Window.partitionBy($"requestDate").orderBy('freq_count desc)

    val rankByVisit = rank().over(visitorWindow)

    freq_visitors.select('*, rankByVisit as 'rank).filter($"rank" <= topN).drop("rank")
  }

}
