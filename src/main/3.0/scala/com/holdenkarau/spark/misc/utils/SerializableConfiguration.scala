package com.holdenkarau.spark.misc.utils

import org.apache.spark.util.{SerializableConfiguration => OSSConfig}
import org.apache.hadoop.conf.{Configuration => HadoopConf}

class SerializableConfiguration(@transient hadoopConf: HadoopConf) extends OSSConfig(hadoopConf) {
}
