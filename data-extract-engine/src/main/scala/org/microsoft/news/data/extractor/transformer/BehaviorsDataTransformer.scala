package org.microsoft.news.data.extractor.transformer

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.microsoft.news.data.extractor.data_schemas.{Behaviors, News}

object BehaviorsDataTransformer {

  def dataTransformer(behaviorsRDD: RDD[String]): RDD[Behaviors] = {

    val transformRDD: RDD[Behaviors] = behaviorsRDD
      .map(row => row.split("\t", -1))
      .map(
        col => {

          val impressionId: String = col(0)
          val userId: String = col(1)
          val time: String = col(2)
          val history: String = col(3)
          val impression: String = col(4)

          val clickCounter: Long = impression.split(" ")
            .map {
              str => {
                if (str.endsWith("1")) {
                  1
                } else {
                  0
                }
              }
            }.foldLeft(0L)(_ + _)

          val nonClickCounter: Long = impression.split(" ")
            .map {
              str => {
                if (str.endsWith("0")) {
                  1
                } else {
                  0
                }
              }
            }.foldLeft(0L)(_ + _)

          Behaviors(
            impressionId,
            userId,
            time,
            history,
            impression,
            clickCounter,
            nonClickCounter
          )
        }
      )
    transformRDD.persist(StorageLevel.MEMORY_AND_DISK)
  }

}
