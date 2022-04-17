package org.microsoft.news.dataloader

trait DataReaderTrait {
  def readDataToRDD(): Any
}
