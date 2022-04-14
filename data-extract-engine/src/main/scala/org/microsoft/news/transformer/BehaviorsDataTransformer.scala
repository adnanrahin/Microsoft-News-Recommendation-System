package org.microsoft.news.transformer

import org.apache.spark.rdd.RDD
import org.microsoft.news.entity.{Behaviors, News}

object BehaviorsDataTransformer {

  def dataTransformer(newsRDD: RDD[String]): RDD[Behaviors] = {

    val transformRDD: RDD[Behaviors] = newsRDD
      .map(row => row.split("\t", -1))
      .map(
        str => {

        }
      )
  }

}
