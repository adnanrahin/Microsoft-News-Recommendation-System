package org.microsoft.news

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.microsoft.news.entity.News

import scala.util.parsing.json.JSON

object DataExtractEngineHelper {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("DataExtractEngine")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val path: String = args(0)

    val dataPath: String = s"$path/news.tsv"

    val newsRDD: RDD[String] = sc.textFile(dataPath)

    val transformation: RDD[News] = newsRDD
      .map(row => row.split("\t", -1))
      .map(
        str => {

          val titlesEntities = JSON.parseFull(str(str.length - 2)).toList
          val titlesEntitiesFlattenRow = extractEntities(titlesEntities)

          val abstractEntities = JSON.parseFull(str(str.length - 1)).toList
          val abstractEntitiesFlattenRow = extractEntities(abstractEntities)

          News(
            str(0),
            str(1),
            str(2),
            str(3),
            str(4),
            str(5),
            titlesEntitiesFlattenRow(0),
            titlesEntitiesFlattenRow(1),
            titlesEntitiesFlattenRow(2),
            titlesEntitiesFlattenRow(3),
            titlesEntitiesFlattenRow(4),
            titlesEntitiesFlattenRow(5),
            abstractEntitiesFlattenRow(0),
            abstractEntitiesFlattenRow(1),
            abstractEntitiesFlattenRow(2),
            abstractEntitiesFlattenRow(3),
            abstractEntitiesFlattenRow(4),
            abstractEntitiesFlattenRow(5)
          )
        }
      )

    val df = spark.createDataFrame(transformation)
    dataWriter(df,"C:\\Users\\rahin\\source-code\\Scala\\Microsoft-News-Recommendation-System", "news")

  }

  final def dataWriter(dataFrame: DataFrame, dataPath: String, directoryName: String): Unit = {

    val destinationDirectory: String = dataPath + "/" + directoryName

    dataFrame
      .write
      .mode(SaveMode.Overwrite)
      .parquet(destinationDirectory)
  }



}
