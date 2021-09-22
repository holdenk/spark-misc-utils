package com.holdenkarau.spark.misc.utils

import scala.reflect._

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

object AltEncoder {
  def COMPRESSED_BINARY: Encoder[Array[Byte]] = {
    new ExpressionEncoder[Array[Byte]](
      schema = new StructType().add("value", BinaryType),
      flat = true,
      serializer = Seq(CompressedArrayEncoder(
        BoundReference(0, ObjectType(classOf[AnyRef]), nullable = true))),
      deserializer = CompressedArrayDecoder(
        Cast(GetColumnByOrdinal(0, BinaryType), BinaryType)),
      clsTag = classTag[Array[Byte]]
    )
  }
}
