package com.holdenkarau.spark.misc.utils

import java.util.zip._
import java.io._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types._


case class CompressedArrayEncoder(child: Expression) extends UnaryExpression
    with NonSQLExpression {
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

  // New in 3.2
  def withNewChildInternal(newChild: Expression) = {
    copy(child = newChild)
  }
}
