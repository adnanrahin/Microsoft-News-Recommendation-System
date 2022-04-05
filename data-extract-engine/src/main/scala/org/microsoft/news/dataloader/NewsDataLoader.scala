package org.microsoft.news.dataloader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.microsoft.news.entity.News
import org.microsoft.news.transformer.NewsDataTransformer

class NewsDataLoader(dataPath: String, spark: SparkSession) extends DataLoader {
  override def loadRDD(): RDD[News] = {
    val sparkContext = this.spark.sparkContext
    val newsRDD: RDD[String] = sparkContext.textFile(dataPath)
    NewsDataTransformer.dataTransformer(newsRDD)
  }
}
