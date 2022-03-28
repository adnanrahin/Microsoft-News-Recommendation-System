package org.ubi.quant.analysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataExtractEngineHelper {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("DataExtractEngine")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val input: String = args(0)
    val output: String = args(1)

    val df = spark.read.csv(input)

    println(df.show(20))

    df
      .write
      .mode(SaveMode.Overwrite)
      .parquet(output)

  }

}
