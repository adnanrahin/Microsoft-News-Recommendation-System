package org.microsoft.news.data_reader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.microsoft.news.data_schemas.News
import org.microsoft.news.transformer.NewsDataTransformer

class NewsDataReader(dataPath: String, spark: SparkSession) extends DataReaderTrait {
  override def readDataToRDD(): RDD[News] = {
    val sparkContext = this.spark.sparkContext
    val newsRDD: RDD[String] = sparkContext.textFile(dataPath)
    NewsDataTransformer.dataTransformer(newsRDD)
  }
}
