package org.microsoft.news.transformer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.microsoft.news.data_schemas.Behaviors
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter
import scala.collection.mutable.Stack

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
      "N8668 N39081 N65259 N79529 N73408 N43615 N29379 N32031 N110232 N101921 N12614 N129591 N105760 N60457 N1229 N64932\t" +
      "N78206-0 N26368-0 N7578-0 N58592-0 N19858-0 N58258-0 N18478-0 N2591-0 N97778-0 N32954-0 N94157-1 " +
      "N39404-0 N108809-0 N78699-1 N71090-1 N40282-0 N31174-1 N37924-0 N27822-0"
    )

    val rdd: RDD[String] = sparkContext.parallelize(actual)

    val expected = Behaviors(
      "1",
      "U87243",
      "11/10/2019 11:30:54 AM",
      "",
      "",
      15,
      3
    )

    val actualBehaviors = BehaviorsDataTransformer.dataTransformer(rdd)

    assert(actualBehaviors.collect().head == expected)

  }

}
