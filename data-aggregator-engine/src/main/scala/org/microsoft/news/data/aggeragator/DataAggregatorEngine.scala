package org.microsoft.news.data.aggeragator

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

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

  }

}
