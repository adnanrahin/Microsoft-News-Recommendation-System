package org.ubi.quant.analysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataExtractEngine {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("DataExtractEngine")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val df = spark.read.csv("D:\\ML-DATA-SET\\ubiquant-market-prediction\\train.csv")

    println(df.show(20))

    df
      .write
      .mode(SaveMode.Overwrite)
      .parquet("C:\\Users\\rahin\\source-code\\Scala\\Ubi-Quant-Market-Prediction\\data-set")

  }

}
