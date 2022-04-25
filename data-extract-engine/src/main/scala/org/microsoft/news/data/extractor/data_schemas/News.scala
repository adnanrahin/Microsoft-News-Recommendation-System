package org.microsoft.news.data.extractor.data_schemas

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
                 tittleOccurrenceOffsets: String,
                 titleSurfaceForms: String,
                 abstractLabel: String,
                 abstractType: String,
                 abstractWikiDataId: String,
                 abstractConfidence: String,
                 abstractOccurrenceOffsets: String,
                 abstractSurfaceForms: String,
               )
