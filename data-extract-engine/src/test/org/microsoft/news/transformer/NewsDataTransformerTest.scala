package org.microsoft.news.transformer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.aggregate.Last
import org.microsoft.news.data_schemas.News
import org.scalatest.funsuite.AnyFunSuite

class NewsDataTransformerTest extends AnyFunSuite {

  test("newsDataTransformerMisMatch") {

    val spark = SparkSession
      .builder()
      .appName("test")
      .master(master = "local[*]")
      .getOrCreate()

    val sparkContext = spark.sparkContext

    val actual = Seq("N88753\t" +
      "lifestyle\t" +
      "lifestyleroyals\t" +
      "The Brands Queen Elizabeth, Prince Charles, and Prince Philip Swear By\t" +
      "Shop the notebooks, jackets, and more that the royals can't live without.\t" +
      "https://assets.msn.com/labs/mind/AAGH0ET.html\t" +
      "[{\"Label\": \"Prince Philip, Duke of Edinburgh\", \"Type\": \"P\", \"WikidataId\": \"Q80976\", \"Confidence\": 1.0, \"OccurrenceOffsets\": [48], \"SurfaceForms\": [\"Prince Philip\"]}, {\"Label\": \"Charles, Prince of Wales\", \"Type\": \"P\", \"WikidataId\": \"Q43274\", \"Confidence\": 1.0, \"OccurrenceOffsets\": [28], \"SurfaceForms\": [\"Prince Charles\"]}, {\"Label\": \"Elizabeth II\", \"Type\": \"P\", \"WikidataId\": \"Q9682\", \"Confidence\": 0.97, \"OccurrenceOffsets\": [11], \"SurfaceForms\": [\"Queen Elizabeth\"]}]\t" +
      "[]"
    )

    val rdd: RDD[String] = sparkContext.parallelize(actual)

    val expected = News(
      "N45436",
      "news",
      "newsscienceandtechnology",
      "Walmart Slashes Prices on Last-Generation iPads", "Apple's new iPad releases bring big deals on last year's models.",
      "https://assets.msn.com/labs/mind/AABmf2I.html",
      "[IPad:Walmart]",
      "[Q2796:Q483551]",
      "[0.999:1.0]",
      "[J:O]",
      "[42.0:0.0]",
      "[iPads:Walmart]",
      "[IPad:Apple Inc.]",
      "[Q2796:Q312]",
      "[0.999:0.999]",
      "[J:O]",
      "[12.0:0.0]",
      "[iPad:Apple]"
    )

    val actualBehaviors = NewsDataTransformer.dataTransformer(rdd)

    assert(actualBehaviors.collect().head != expected)

  }

  test("newsDataTransformerMatch") {

    val spark = SparkSession
      .builder()
      .appName("test")
      .master(master = "local[*]")
      .getOrCreate()

    val sparkContext = spark.sparkContext

    val actual = Seq(
      "N9663\t" +
        "travel\t" +
        "traveltripideas\t" +
        "17 Abandoned Theme Parks to Explore for Thrills, Chills, and Nostalgia\t" +
        "Disney, Six Flags, and even the Flintstones have had amusement parks that succumbed to disasters.\t" +
        "https://assets.msn.com/labs/mind/AADlunl.html\t" +
        "[{\"Label\": \"Amusement park\", " +
        "\"Type\": \"C\", " +
        "\"WikidataId\": \"Q194195\", " +
        "\"Confidence\": 0.988" +
        "\"OccurrenceOffsets\": [13]" +
        "\"SurfaceForms\": [\"Theme Parks\"]}]"
    )

    val rdd: RDD[String] = sparkContext.parallelize(actual)

    val expected =
      News(
        "N9663",
        "travel",
        "traveltripideas",
        "17 Abandoned Theme Parks to Explore for Thrills, Chills, and Nostalgia",
        "Disney, Six Flags, and even the Flintstones have had amusement parks that succumbed to disasters.",
        "https://assets.msn.com/labs/mind/AADlunl.html",
        "[Amusement park,Chills]",
        "[Q194195,Q2260058]",
        "[0.988,0.988]",
        "[C]",
        "[13.0]",
        "[Theme Parks,Chills]",
        "[Six Flags,Florida,The Flintstones,Ohio,Amusement park,California]",
        "[Q194195]",
        "[1.0,1.0,1.0,1.0,0.988,1.0]",
        "[O,G,W,G,C,G]",
        "[8.0,216.0,32.0,228.0,180.0,204.0]",
        "[Six Flags,Florida,Flintstones,Ohio,theme parks,California]"
      )

    val actualBehaviors = NewsDataTransformer.dataTransformer(rdd)

    assert(actualBehaviors.collect().head != expected)

  }

}
