package com.secureworks.codingchallenge

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkFiles
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, rank, regexp_extract, desc}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat


@SerialVersionUID(1000L)
case class DataProcessor(spark: SparkSession, args: Array[String]) extends Serializable {
  @transient lazy private val logger = Logger.getLogger(getClass)
  logger.setLevel(Level.INFO)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  private val trueValue = true
  private val falseValue = false


  logger.info("Extracting parameters from the console Args.....")
  private val parameters = if(args.length >= 2) {
    ProcessUtils.extractParameters(args)
  } else {
    logger.info("no parameters were passed from the command line")
    Map.empty[String, Any]
  }

  logger.info("extracted parameters from the Console....")
  parameters.foreach(p => println(s"Value for argument ${p._1} = ${p._2}") )

  private val sourceUrl = parameters.getOrElse("sourceUrl","ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz").toString
  private val topRecordsSize = Integer.parseInt(parameters.getOrElse("topRecordsSize", 10).asInstanceOf[String])
  private val defaultResultSize = Integer.parseInt(parameters.getOrElse("consoleResultSize", 15).asInstanceOf[String])

  //Patterns to extract from semi-structured Log data
  private val extractPatterns = Map(
    "hostPattern" -> """(\S+\.[\S+\.]+\S+)""".r,
    "tsPattern" -> """\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]""".r,
    "requestPattern" -> """\"(\S+)\s(\S+)\s*(\S*)\"""".r,
    "httpCodePattern" -> """\s(\d{3})\s""".r,
    "responseByteSizePattern" -> """\s(\d+)$""".r
  )

   def process() = {
    logger.info("Starting the log processing in spark......")

    val sc = spark.sparkContext
    sc.addFile(sourceUrl)

     logger.info("Adding source file to spark context to be to download on all executor nodes...")
     val localPath = SparkFiles.get(sourceUrl.split("/").last)

    logger.info("importing spark implicits for internal conversions....")
    import spark.implicits._

    logger.info("creating source rdd with flattening methods from ftp url....")
    val srcRdd = sc.textFile(localPath)
      .filter(!_.isEmpty)
      .map(s => {
        val matchedObjects = ProcessUtils.extractPattern(extractPatterns, s)
        val modifiedObjects = matchedObjects.map(t =>
          if(t._1 == "tsPattern") {
            t._2 match {
              case "" => t._2.trim
              case _ => ProcessUtils.getFormattedDate(t._2.toString).trim
            }
          } else {t._2.trim}
        )
        Row.fromSeq(modifiedObjects.toList)
      })

    srcRdd.repartition(50).persist(StorageLevel.MEMORY_AND_DISK)

    logger.info(s"Total records from the source rdd created on GZip file from ftpSource: ${srcRdd.count.toString} ...")

    val baseSchema = StructType(
      List(
        StructField("requestDate", StringType, trueValue),
        StructField("visitor", StringType, trueValue),
        StructField("totalBytes", StringType, trueValue),
        StructField("responseCode", StringType, trueValue),
        StructField("requestUrl", StringType, trueValue)
      )
    )

    logger.info("creating dataFrame from rdd to derive the metrics...")
    val src_df = spark.createDataFrame(srcRdd, baseSchema)

    src_df.show(defaultResultSize, falseValue)

    val base_df = src_df.select(col("visitor"), col("requestDate"),
      regexp_extract($"requestUrl", """\"(\S+)\s(\S+)\s*(\S*)\"""", 1).alias("method"),
      regexp_extract($"requestUrl", """\"(\S+)\s(\S+)\s*(\S*)\"""", 2).alias("endpoint"),
      regexp_extract($"requestUrl", """\"(\S+)\s(\S+)\s*(\S*)\"""", 3).alias("protocol"),
      col("responseCode").cast("integer").alias("response"))
      .filter($"visitor" =!= "" || $"response".isNotNull)

    srcRdd.unpersist()

    base_df.persist(StorageLevel.MEMORY_AND_DISK)

    logger.info(s"Total count for data frame after parsing all the data: ${base_df.count.toString}")

    base_df.show(defaultResultSize, falseValue)

    val topVisitsByAddress = ProcessUtils.getTopVisitsFromDF(base_df, topRecordsSize, Some("visitor"))
    val topVisitsByUrl = ProcessUtils.getTopVisitsFromDF(base_df, topRecordsSize, Some("endPoint"))


    logger.info("showing top visits by Visitors.....")

     topVisitsByAddress.show(defaultResultSize, falseValue)

    logger.info("showing top visits by Url.....")

     topVisitsByUrl.show(defaultResultSize, falseValue)

    spark.catalog.clearCache()

  }
}
