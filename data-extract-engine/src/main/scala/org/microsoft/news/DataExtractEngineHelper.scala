package org.microsoft.news

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.microsoft.news.transformer.NewsDataTransformer

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

    val transformation = NewsDataTransformer.dataTransformer(newsRDD)

    val df = spark.createDataFrame(transformation)

    dataWriter(
      df,
      "C:\\Users\\rahin\\source-code\\Scala\\Microsoft-News-Recommendation-System",
      "news")

  }

  final def dataWriter(dataFrame: DataFrame, dataPath: String, directoryName: String): Unit = {

    val destinationDirectory: String = dataPath + "/" + directoryName

    dataFrame
      .write
      .mode(SaveMode.Overwrite)
      .parquet(destinationDirectory)
  }


}
