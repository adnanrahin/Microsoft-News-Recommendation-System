package org.microsoft.news.dataloader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.microsoft.news.entity.{Behaviors, News}
import org.microsoft.news.transformer.{BehaviorsDataTransformer, NewsDataTransformer}

class BehaviorsDataLoader(dataPath: String, spark: SparkSession) extends DataLoader {
  override def loadRDD(): RDD[Behaviors] = {
    val sparkContext = this.spark.sparkContext
    val behaviorsRDD: RDD[String] = sparkContext.textFile(dataPath)
    BehaviorsDataTransformer.dataTransformer(behaviorsRDD)
  }
}
