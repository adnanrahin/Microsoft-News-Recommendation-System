package org.microsoft.news.dataloader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class NewsDataLoader(dataPath: String, spark: SparkSession) extends DataLoader {
  override def loadRDD(): RDD[String] = {
    val sparkContext = this.spark.sparkContext
    val newsRDD: RDD[String] = sparkContext.textFile(dataPath)
    newsRDD
  }
}
