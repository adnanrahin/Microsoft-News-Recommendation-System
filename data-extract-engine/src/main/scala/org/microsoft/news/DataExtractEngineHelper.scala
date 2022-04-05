package org.microsoft.news

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.parsing.json.JSON

object DataExtractEngineHelper {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("DataExtractEngine")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val path: String = args(0)

    val dataPath: String = s"$path/news.tsv"

    val newsRDD: RDD[String] = sc.textFile(dataPath)

    newsRDD
      .map(row => row.split("\t", -1))
      .foreach(
        str => {
          val titlesEntities = JSON.parseFull(str(str.length - 1)).toList

          titlesEntities.map {
            list => {
              val row = {
                val label = list.asInstanceOf[List[Map[String, Any]]].map(map => map("Label").toString)
                val wikiDataId = list.asInstanceOf[List[Map[String, Any]]].map(map => map("WikidataId").toString)
                val confidence = list.asInstanceOf[List[Map[String, Any]]].map(map => map("Confidence").toString)
                val titleType = list.asInstanceOf[List[Map[String, Any]]].map(map => map("Type").toString)
                val occurrenceOffsets = list.asInstanceOf[List[Map[String, Any]]].map(map => map("OccurrenceOffsets").toString)
                val surfaceForms = list.asInstanceOf[List[Map[String, Any]]].map(map => map("SurfaceForms").toString)


                val wikiDataIdStringFilter = stringFilterRemoveListWord(wikiDataId)
                val occurrenceOffsetsStringFilter = stringFilterRemoveListWord(occurrenceOffsets)
                val surfaceFormsStringFilter = stringFilterRemoveListWord(surfaceForms)


                val wikiDataIdString = isStringOrNone(wikiDataIdStringFilter)
                val labelString = isStringOrNone(label)
                val confidenceString = isStringOrNone(confidence)
                val titleTypeString = isStringOrNone(titleType)
                val occurrenceOffsetsString = isStringOrNone(occurrenceOffsetsStringFilter)
                val surfaceFormsString = isStringOrNone(surfaceFormsStringFilter)

                List(labelString,
                  wikiDataIdString,
                  confidenceString,
                  titleTypeString,
                  occurrenceOffsetsString,
                  surfaceFormsString)

              }

              println(row.mkString("\t"))
              println("\n")

            }
          }

        }
      )

  }

  def isStringOrNone(strList: List[String]): Any = {
    if (strList.isEmpty) None else "[" + strList.mkString(",") + "]"
  }

  def stringFilterRemoveListWord(strList: List[String]): List[String] = {
    strList.map(entity =>
      entity
        .toString
        .replace("List(", "")
        .replace(")", "")
    )
  }

}
