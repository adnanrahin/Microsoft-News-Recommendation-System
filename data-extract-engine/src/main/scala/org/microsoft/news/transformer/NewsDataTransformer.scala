package org.microsoft.news.transformer

object NewsDataTransformer {

  private def extractEntities(titlesEntities: List[Any]): Array[String] = {
    val extractedTitle: List[String] = titlesEntities.map {
      list =>
        val row: String = {
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
