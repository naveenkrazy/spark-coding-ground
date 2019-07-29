package com.secureworks.codingchallenge

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, rank}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.matching.Regex

object ProcessUtils {

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

  def extractPattern(patternMap: Map[String, Regex], sourceString: String): Map[String, String] = {

    patternMap.map(p => p._1 -> p._2.findFirstIn(sourceString).getOrElse(""))

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
      .select(col("requestDate"), col(s"$freqColumn"), col("count").alias("freq_count"))

    val visitorWindow = Window.partitionBy(col("requestDate")).orderBy(desc("freq_count"))

    val rankByVisit = rank().over(visitorWindow)

    freq_visitors.select(col("*"), rankByVisit as 'rank).filter(col("rank") <= topN).drop("rank")
  }

}
