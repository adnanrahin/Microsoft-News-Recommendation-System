package org.microsoft.news.entity

case class News(
               newsId: String,
               category: String,
               subCategory: String,
               title: String,
               abstractDescription: String,
               url: String,
               titleLabel: String,
               titleType: String,
               titleWikiDataId: String,
               titleConfidence: String,

               )
