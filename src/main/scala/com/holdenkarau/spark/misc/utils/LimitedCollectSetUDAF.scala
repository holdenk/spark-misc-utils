package com.holdenkarau.spark.misc.utils

import scala.collection.mutable.HashSet

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

class LimitedCollectSetUDAF(maxElems: Int = 5000) extends Aggregator[Int, HashSet[Int], List[Int]] {

  def zero: HashSet[Int] = HashSet.empty[Int]
  def reduce(buffer: HashSet[Int], elem: Int): HashSet[Int] = {
    println(s"Adding $elem with current buffer $buffer")
    if (buffer.size < maxElems) {
      buffer += elem
    }
    println(s"Got buffer $buffer")
    buffer
  }

  def merge(a1: HashSet[Int], a2: HashSet[Int]): HashSet[Int] = {
    println(s"Merging $a1 $a2")
    if (a2.size == maxElems) {
      a2
    } else {
      val elemItr = a2.iterator
      while (a1.size < maxElems && elemItr.hasNext) {
        a1 += elemItr.next()
      }
      a1
    }
  }

  override def finish(a: HashSet[Int]): List[Int] = {
    println(s"Finished $a")
    a.toList
  }

  override def bufferEncoder: Encoder[HashSet[Int]] = Encoders.kryo[HashSet[Int]]

  // This encoder is "tricky" if we pick the kyro encoder we get "junk" results.
  override def outputEncoder: Encoder[List[Int]] = Encoders.product[List[Int]]
}
