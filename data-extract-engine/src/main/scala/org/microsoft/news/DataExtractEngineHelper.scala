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

    val l: List[Any] = List(Map("A" -> "OMEGA-TEST", "B" -> List('A', 'B')), "String Test")

    //l.foreach(list => println(list))

    /* l.foreach {
       value => {
         val printVal = value match {
           case map: Map[String, Any] => {
             map.map {
               case (key, mapVal) => {
                 println(key + " : " + mapVal)
               }
             }
           }
           case _ => println(value)
         }
       }
     }
 */

    val sc = spark.sparkContext
    val path: String = args(0)

    val dataPath: String = s"$path/news.tsv"

    val testRdd: RDD[String] = sc.textFile(dataPath)

    print(testRdd.count())

    testRdd
      .map(row => row.split("\t", -1))
      .foreach(
        str => {
          val test = JSON.parseFull(str(str.length - 1)).toList

          test.map(
            value => {
              val printVal = value match {
                case map: Map[String, Any] => {
                  map.map {
                    case (key, mapVal) => {
                      println(key + " : " + mapVal)
                    }
                  }
                }
                case _ => println(value)
              }
            }
          )

        }
      )

  }

}
