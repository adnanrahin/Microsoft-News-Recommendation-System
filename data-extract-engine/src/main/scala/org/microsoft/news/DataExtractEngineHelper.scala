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
              val label = list.asInstanceOf[List[Map[String, Any]]].map(map => map("Label").toString)
              val wikiDataId = list.asInstanceOf[List[Map[String, Any]]].map(map => map("WikidataId").toString)
              val confidence = list.asInstanceOf[List[Map[String, Any]]].map(map => map("Confidence").toString)
              val titleType = list.asInstanceOf[List[Map[String, Any]]].map(map => map("Type").toString)
              val occurrenceOffsets = list.asInstanceOf[List[Map[String, Any]]].map(map => map("OccurrenceOffsets").toString)
              val surfaceForms = list.asInstanceOf[List[Map[String, Any]]].map(map => map("SurfaceForms").toString)

              val wikiDataIdStringFilter = wikiDataId
                .map(entity =>
                  entity
                    .toString
                    .replace("List(", "")
                    .replace(")", "")
                )

              val occurrenceOffsetsStringFilter = occurrenceOffsets
                .map(entity =>
                  entity
                    .toString
                    .replace("List(", "")
                    .replace(")", "")
                )


              val surfaceFormsStringFilter = surfaceForms
                .map(entity =>
                  entity
                    .toString
                    .replace("List(", "")
                    .replace(")", "")
                )

              val wikiDataIdString =
                if (wikiDataIdStringFilter.nonEmpty)
                  "[" + wikiDataIdStringFilter.mkString(",") + "]"
                else None

              val labelString =
                if (label.nonEmpty)
                  "[" + label.mkString(",") + "]"
                else None

              val confidenceString =
                if (confidence.nonEmpty)
                  "[" + confidence.mkString(",") + "]"
                else None

              val titleTypeString =
                if (titleType.nonEmpty)
                  "[" + titleType.mkString(",") + "]"
                else None

              val occurrenceOffsetsString =
                if (occurrenceOffsetsStringFilter.nonEmpty)
                  "[" + occurrenceOffsetsStringFilter.mkString(",") + "]"
                else None

              val surfaceFormsString =
                if (surfaceFormsStringFilter.nonEmpty)
                  "[" + surfaceFormsStringFilter.mkString(",") + "]"
                else None

              println("LABEL: " + labelString)
              println("WIKIDATAID: " + wikiDataIdString)
              println("CONFIDENCE: " + confidenceString)
              println("TITLETYPE: " + titleTypeString)
              println("OCCURRENCEOFFSETS: " + occurrenceOffsetsString)
              println("SURFACEFORMS: " + surfaceFormsString)

              println()
            }
          }

        }
      )

  }

}
