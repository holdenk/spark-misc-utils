package com.holdenkarau.spark.misc.utils

import com.holdenkarau.spark.testing._
import org.scalatest.funsuite.AnyFunSuite
import org.scalacheck.Arbitrary
import org.scalacheck.Prop.forAll
import org.scalatestplus.scalacheck.Checkers
import org.apache.spark.sql._
import org.apache.spark.sql.functions

case class Bloop(id: Int, name: String, fav: Int)

class LimitedCollectSetUDAFSuite extends AnyFunSuite with SharedSparkContext with Checkers {
  val testElems = List(
    Bloop(1, "a", 42),
    Bloop(2, "b", 42),
    Bloop(3, "c", 7),
    Bloop(3, "c", 420))


  test("Simple test") {
    val sqlContext = SparkSession.builder.getOrCreate().sqlContext

    import sqlContext.implicits._

    // Register the UDAF
    val limited_collect_set = functions.udaf(new LimitedCollectSetUDAF(2))


    val testDF = sqlContext.createDataFrame(testElems)
    testDF.show()
    testDF.select(limited_collect_set($"fav")).show()
  }

}
