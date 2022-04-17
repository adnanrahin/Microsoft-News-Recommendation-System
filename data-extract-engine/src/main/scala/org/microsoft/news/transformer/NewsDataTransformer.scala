package org.microsoft.news.transformer

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.microsoft.news.constant.Constant._
import org.microsoft.news.data_schemas.News

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

          val newsId: String = str(0)
          val category: String = str(1)
          val subCategory: String = str(2)
          val title: String = str(3)
          val abstractDescription: String = str(4)
          val url: String = str(5)
          val titleLabel: String = titlesEntitiesFlattenRow(0)
          val titleType: String = titlesEntitiesFlattenRow(1)
          val titleWikiDataId: String = titlesEntitiesFlattenRow(2)
          val titleConfidence: String = titlesEntitiesFlattenRow(3)
          val tittleOccurrenceOffsets: String = titlesEntitiesFlattenRow(4)
          val titleSurfaceForms: String = titlesEntitiesFlattenRow(5)
          val abstractLabel: String = abstractEntitiesFlattenRow(0)
          val abstractType: String = abstractEntitiesFlattenRow(1)
          val abstractWikiDataId: String = abstractEntitiesFlattenRow(2)
          val abstractConfidence: String = abstractEntitiesFlattenRow(3)
          val abstractOccurrenceOffsets: String = abstractEntitiesFlattenRow(4)
          val abstractSurfaceForms: String = abstractEntitiesFlattenRow(5)


          News(
            newsId,
            category,
            subCategory,
            title,
            abstractDescription,
            url,
            titleLabel,
            titleType,
            titleWikiDataId,
            titleConfidence,
            tittleOccurrenceOffsets,
            titleSurfaceForms,
            abstractLabel,
            abstractType,
            abstractWikiDataId,
            abstractConfidence,
            abstractOccurrenceOffsets,
            abstractSurfaceForms
          )
        }
      )
    transformRDD.persist(StorageLevel.MEMORY_AND_DISK)
  }


  private def extractEntities(titlesEntities: List[Any]): Array[String] = {
    val extractedTitle: List[String] = titlesEntities.map {
      title =>
        val entity: String = {
          val label = title.asInstanceOf[List[Map[String, Any]]].map(map => map(LABEL).toString)
          val wikiDataId = title.asInstanceOf[List[Map[String, Any]]].map(map => map(WIKIDATAID).toString)
          val confidence = title.asInstanceOf[List[Map[String, Any]]].map(map => map(CONFIDENCE).toString)
          val titleType = title.asInstanceOf[List[Map[String, Any]]].map(map => map(TYPE).toString)
          val occurrenceOffsets = title.asInstanceOf[List[Map[String, Any]]].map(map => map(OCCURRENCEOFFSETS).toString)
          val surfaceForms = title.asInstanceOf[List[Map[String, Any]]].map(map => map(SURFACEFORMS).toString)


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
        entity
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
