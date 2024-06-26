package com.holdenkarau.spark.misc.utils

import com.holdenkarau.spark.testing._
import org.scalatest.funsuite.AnyFunSuite
import org.scalacheck.Arbitrary
import org.scalacheck.Prop.forAll
import org.scalatestplus.scalacheck.Checkers
import org.apache.spark.sql._

class AltEncoderSuite extends AnyFunSuite with SharedSparkContext with Checkers {
  test("Simple test") {
    val sqlContext = SparkSession.builder.getOrCreate().sqlContext

    import sqlContext.implicits.{newByteArrayEncoder => _, _} // See SQLImplicits.scala

    implicit def encode[Array[Dataset]] = AltEncoder.COMPRESSED_BINARY

    val ds = sqlContext.createDataset[Array[Byte]](List(Array[Byte](1)))
    ds.repartition(10)
    assert(ds.count == 1)
  }

 test("Semi-Simple test") {
    val sqlContext = SparkSession.builder.getOrCreate().sqlContext


    import sqlContext.implicits.{newByteArrayEncoder => _, _} // See SQLImplicits.scala

    implicit def encode[Array[Dataset]] = AltEncoder.COMPRESSED_BINARY

    val ds = sqlContext.createDataset[Array[Byte]](List(Array[Byte](1)))
    ds.repartition(10)
    assert(ds.map(_ == null).count == 1)
  }


  test("test generating Datasets[Array[Byte]]]") {
    val sqlContext = SparkSession.builder.getOrCreate().sqlContext

    import sqlContext.implicits.{newIntEncoder, newLongEncoder} // See SQLImplicits.scala

    implicit def encode[Array[Dataset]] = AltEncoder.COMPRESSED_BINARY

    val property =
      forAll(
        DatasetGenerator.genDataset[Array[Byte]](sqlContext)(
          Arbitrary.arbitrary[Array[Byte]])) {
        dataset =>
          // Repartition to force serialization.
          val newSet = dataset.repartition(10)
          (newSet.except(dataset).count == 0 && dataset.map(_.length).count() == dataset.count())
      }

    check(property)
  }
}
