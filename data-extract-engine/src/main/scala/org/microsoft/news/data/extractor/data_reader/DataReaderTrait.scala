package org.microsoft.news.data.extractor.data_reader

trait DataReaderTrait {
  def readDataToRDD(): Any
}
