package org.microsoft.news.transformer

import org.apache.spark.rdd.RDD
import org.microsoft.news.entity.{Behaviors, News}

object BehaviorsDataTransformer {

  def dataTransformer(newsRDD: RDD[String]): RDD[Behaviors] = {

    val transformRDD: RDD[Behaviors] = newsRDD
      .map(row => row.split("\t", -1))
      .map(
        col => {

          val impressionId = col(0)
          val userId = col(1)
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
            clickCounter.toString,
            nonClickCounter.toString
          )
        }
      )
    transformRDD
  }

}
