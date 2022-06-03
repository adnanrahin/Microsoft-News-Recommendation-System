package org.microsoft.news.data.aggeragator

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.microsoft.news.data.extractor.data_reader.{BehaviorsDataReader, NewsDataReader}
import org.microsoft.news.data.extractor.data_schemas.{Behaviors, News}
import org.microsoft.news.data.extractor.data_writer.DataFileWriterLocal

object DataAggregatorEngine {
 
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val initialDataPath: String = args(0)
    val transformDataPath: String = args(1)
    val masterNode = if (args.length < 3) "local[*]" else args(2)

    val spark = SparkSession
      .builder()
      .appName("DataExtractEngine")
      .master(master = masterNode)
      .getOrCreate()

    val newsDataLoader = new NewsDataReader(s"$initialDataPath/news.tsv", spark)
    val newsRDD: RDD[News] = newsDataLoader.readDataToRDD()
    val newsDF = spark.createDataFrame(newsRDD)

    DataFileWriterLocal.dataWriter(dataFrame = newsDF,
      dataPath = transformDataPath,
      directoryName = "newsdata")

    val behaviorsDataLoader = new BehaviorsDataReader(s"$initialDataPath/behaviors.tsv", spark)
    val behaviorsRDD: RDD[Behaviors] = behaviorsDataLoader.readDataToRDD()
    val behaviorsDF = spark.createDataFrame(behaviorsRDD)

    DataFileWriterLocal.dataWriter(dataFrame = behaviorsDF,
      dataPath = transformDataPath,
      directoryName = "behaviorsdata")

  }

}
