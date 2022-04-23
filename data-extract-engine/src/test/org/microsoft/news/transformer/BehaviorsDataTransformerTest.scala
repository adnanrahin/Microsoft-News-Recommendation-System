package org.microsoft.news.transformer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.microsoft.news.data_schemas.Behaviors
import org.scalatest.funsuite.AnyFunSuite

class BehaviorsDataTransformerTest extends AnyFunSuite {

  test("testDataTransformer") {

    val spark = SparkSession
      .builder()
      .appName("test")
      .master(master = "local[*]")
      .getOrCreate()

    val sparkContext = spark.sparkContext

    val actual = Seq("1\t" +
      "U87243\t" +
      "11/10/2019 11:30:54 AM\t" +
      "N8668 N39081 N65259 N79529\t" +
      "N78206-0 N26368-0 N7578-0 N94157-1 N39404-0 N108809-0 N78699-1 N71090-1 N40282-0 N31174-1"
    )

    val rdd: RDD[String] = sparkContext.parallelize(actual)

    val expected = Behaviors(
      "1",
      "U87243",
      "11/10/2019 11:30:54 AM",
      "N8668 N39081 N65259 N79529",
      "N78206-0 N26368-0 N7578-0 N94157-1 N39404-0 N108809-0 N78699-1 N71090-1 N40282-0 N31174-1",
      4,
      6
    )

    val actualBehaviors = BehaviorsDataTransformer.dataTransformer(rdd)

    assert(actualBehaviors.collect().head == expected)

  }

  test("testDataTransformer") {

    val spark = SparkSession
      .builder()
      .appName("test")
      .master(master = "local[*]")
      .getOrCreate()

    val sparkContext = spark.sparkContext

    val actual = Seq("1\t" +
      "U87243\t" +
      "11/10/2019 11:30:54 AM\t" +
      "N8668 N39081 N65259 N79529\t" +
      "N94157-1 N39404-0 N108809-0 N78699-1 N71090-1 N40282-0 N31174-1"
    )

    val rdd: RDD[String] = sparkContext.parallelize(actual)

    val expected = Behaviors(
      "1",
      "U87243",
      "11/10/2019 11:30:54 AM",
      "N8668 N39081 N65259 N79529",
      "N78206-0 N26368-0 N7578-0 N94157-1 N39404-0 N108809-0 N78699-1 N71090-1 N40282-0 N31174-1",
      4,
      6
    )

    val actualBehaviors = BehaviorsDataTransformer.dataTransformer(rdd)

    assert(actualBehaviors.collect().head != expected)

  }

}
