package com.holdenkarau.spark.misc.utils

import org.apache.hadoop.conf.{Configuration => HadoopConf}

import java.io.{ObjectInputStream, ObjectOutputStream}

class SerializableConfiguration(@transient var value: HadoopConf) extends Serializable {
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    value = new HadoopConf(false)
    value.readFields(in)
  }
}
