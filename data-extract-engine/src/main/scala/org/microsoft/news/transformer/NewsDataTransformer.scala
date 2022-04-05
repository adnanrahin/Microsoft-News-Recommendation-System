package org.microsoft.news.transformer

import org.apache.spark.rdd.RDD
import org.microsoft.news.constant.Constant._
import org.microsoft.news.entity.News

import scala.util.parsing.json.JSON

object NewsDataTransformer {

  def dataTransformer(newsRDD: RDD[String]): RDD[News] = {
    val transformRDD: RDD[News] = newsRDD
      .map(row => row.split("\t", -1))
      .map(
        str => {

          val titlesEntities = JSON.parseFull(str(str.length - 2)).toList
          val titlesEntitiesFlattenRow = extractEntities(titlesEntities)

          val abstractEntities = JSON.parseFull(str(str.length - 1)).toList
          val abstractEntitiesFlattenRow = extractEntities(abstractEntities)

          News(
            str(0),
            str(1),
            str(2),
            str(3),
            str(4),
            str(5),
            titlesEntitiesFlattenRow(0),
            titlesEntitiesFlattenRow(1),
            titlesEntitiesFlattenRow(2),
            titlesEntitiesFlattenRow(3),
            titlesEntitiesFlattenRow(4),
            titlesEntitiesFlattenRow(5),
            abstractEntitiesFlattenRow(0),
            abstractEntitiesFlattenRow(1),
            abstractEntitiesFlattenRow(2),
            abstractEntitiesFlattenRow(3),
            abstractEntitiesFlattenRow(4),
            abstractEntitiesFlattenRow(5)
          )
        }
      )
    transformRDD
  }


  private def extractEntities(titlesEntities: List[Any]): Array[String] = {
    val extractedTitle: List[String] = titlesEntities.map {
      list =>
        val row: String = {
          val label = list.asInstanceOf[List[Map[String, Any]]].map(map => map(LABEL).toString)
          val wikiDataId = list.asInstanceOf[List[Map[String, Any]]].map(map => map(WIKIDATAID).toString)
          val confidence = list.asInstanceOf[List[Map[String, Any]]].map(map => map(CONFIDENCE).toString)
          val titleType = list.asInstanceOf[List[Map[String, Any]]].map(map => map(TYPE).toString)
          val occurrenceOffsets = list.asInstanceOf[List[Map[String, Any]]].map(map => map(OCCURRENCEOFFSETS).toString)
          val surfaceForms = list.asInstanceOf[List[Map[String, Any]]].map(map => map(SURFACEFORMS).toString)


          val wikiDataIdStringFilter = stringFilterRemoveListWord(wikiDataId)
          val occurrenceOffsetsStringFilter = stringFilterRemoveListWord(occurrenceOffsets)
          val surfaceFormsStringFilter = stringFilterRemoveListWord(surfaceForms)


          val wikiDataIdString = isStringOrNone(wikiDataIdStringFilter)
          val labelString = isStringOrNone(label)
          val confidenceString = isStringOrNone(confidence)
          val titleTypeString = isStringOrNone(titleType)
          val occurrenceOffsetsString = isStringOrNone(occurrenceOffsetsStringFilter)
          val surfaceFormsString = isStringOrNone(surfaceFormsStringFilter)

          s"$labelString\t$wikiDataIdString\t$confidenceString\t$titleTypeString\t$occurrenceOffsetsString\t$surfaceFormsString"

        }
        row
    }
    extractedTitle.head.split("\t")
  }

  private def isStringOrNone(strList: List[String]): Any = {
    if (strList.isEmpty) None else "[" + strList.mkString(",") + "]"
  }

  private def stringFilterRemoveListWord(strList: List[String]): List[String] = {
    strList.map(entity =>
      entity
        .toString
        .replace("List(", "")
        .replace(")", "")
    )
  }

}
