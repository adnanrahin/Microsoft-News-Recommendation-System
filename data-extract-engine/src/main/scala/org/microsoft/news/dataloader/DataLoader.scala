package org.microsoft.news.dataloader

trait DataLoader {
  def loadRDD(): Any
}
