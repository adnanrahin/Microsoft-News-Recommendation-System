package org.microsoft.news.data.extractor.data_reader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.microsoft.news.data.extractor.data_schemas.{Behaviors, News}
import org.microsoft.news.data.extractor.transformer.{BehaviorsDataTransformer, NewsDataTransformer}

class BehaviorsDataReader(dataPath: String, spark: SparkSession) extends DataReaderTrait {
  override def readDataToRDD(): RDD[Behaviors] = {
    val sparkContext = this.spark.sparkContext
    val behaviorsRDD: RDD[String] = sparkContext.textFile(dataPath)
    BehaviorsDataTransformer.dataTransformer(behaviorsRDD)
  }
}
