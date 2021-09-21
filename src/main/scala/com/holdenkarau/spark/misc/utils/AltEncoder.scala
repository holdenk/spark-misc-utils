package com.holdenkarau.spark.misc.utils

import java.util.zip._
import java.io._

import scala.reflect._

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types._

object AltEncoder {
  val chunkSize = 10240

  def COMPRESSED_BINARY: Encoder[Array[Byte]] = {
    case class CompressedArrayEncoder(child: Expression) extends UnaryExpression with NonSQLExpression {
      override def prettyName = "compressedArrayEncoder"

      override def nullSafeEval(input: Any): Any = {
        if (input == null) {
          return null
        }
        val outputStream = new ByteArrayOutputStream();
        val compressed = new GZIPOutputStream(outputStream);
        compressed.write(input.asInstanceOf[Array[Byte]]);
        compressed.finish();
        compressed.flush();
        compressed.close();
        outputStream.toByteArray()
      }

      override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
        // Code to serialize.
        val input = child.genCode(ctx)
        val javaType = CodeGenerator.javaType(dataType)

        val outputStream = ctx.freshName("outputStream")
        val compressed = ctx.freshName("compressed")

        val code = input.code + code"""
        java.io.ByteArrayOutputStream $outputStream;
        java.util.zip.GZIPOutputStream $compressed;

        try {
        $outputStream = new java.io.ByteArrayOutputStream();
        $compressed = new java.util.zip.GZIPOutputStream($outputStream);
        if (! ${input.isNull} ) {
          ${compressed}.write((byte[]) ${input.value});
          ${compressed}.finish();
          ${compressed}.flush();
          ${compressed}.close();
        }
        } catch (Exception e) {
           throw new RuntimeException("sad" + e);
        }
        final $javaType ${ev.value} =
          ${input.isNull} ? ${CodeGenerator.defaultValue(dataType)} : ${outputStream}.toByteArray();
     """
        ev.copy(code = code, isNull = input.isNull)
      }

      override def dataType: DataType = BinaryType
    }

    case class CompressedArrayDecoder(child: Expression) extends UnaryExpression with NonSQLExpression  {
      override def prettyName = "compressedArrayDecoder"

      override def nullSafeEval(input: Any): Any = {
        val inputBytes = input.asInstanceOf[Array[Byte]]
        val inStream = new GZIPInputStream(new ByteArrayInputStream(inputBytes))
        val buffer = new Array[Byte](chunkSize)
        val out = new ByteArrayOutputStream()

        var len = 0
        len = inStream.read(buffer)
        while (len > 0) {
          out.write(buffer, 0, len)
          len = inStream.read(buffer)
        }

        inStream.close()
        out.close()
        out.toByteArray()
      }

      override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
        // Code to serialize.
        val input = child.genCode(ctx)
        val javaType = CodeGenerator.javaType(dataType)

        val outputStream = ctx.freshName("outStream")
        val inputStream = ctx.freshName("outStream")
        val compressed = ctx.freshName("compressed")

        val code = input.code + code"""
        java.io.ByteArrayInputStream $inputStream;
        java.io.ByteArrayOutputStream $outputStream;
        java.util.zip.GZIPInputStream $compressed;

        try {
        $outputStream = new java.io.ByteArrayOutputStream();
        $inputStream = new java.io.ByteArrayInputStream((byte[]) ${input.value});
        $compressed = new java.util.zip.GZIPInputStream($inputStream);
        if (!${input.isNull}) {
          byte[] buffer = new byte[${chunkSize}];

          int len;
          while ((len = ${compressed}.read(buffer)) > 0) {
            ${outputStream}.write(buffer, 0, len);
          }

          ${compressed}.close();
          ${outputStream}.close();
        }
        } catch (Exception e) {
           throw new RuntimeException("sad" + e);
        }
        final $javaType ${ev.value} =
          ${input.isNull} ? ${CodeGenerator.defaultValue(dataType)} : ${outputStream}.toByteArray();
     """
        ev.copy(code = code, isNull = input.isNull)
      }

      override def dataType: DataType = ObjectType(classOf[Array[Byte]])
    }

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
