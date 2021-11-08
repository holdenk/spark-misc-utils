package com.holdenkarau.spark.misc.utils

import java.io.BufferedInputStream

import collection.JavaConverters._
import collection.mutable.ArrayBuilder

import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.GenericDatumReader
import org.apache.hadoop.conf.{Configuration => HadoopConf}
import org.apache.hadoop.fs._
import org.apache.iceberg.hive.HiveCatalog
import org.apache.iceberg._
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.orc.OrcFile
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._

/**
 * Class so we can test our functions
 */
class IcebergTableCleaner(spark: SparkSession) {
  val catalog = {
    val conf = spark.sparkContext.hadoopConfiguration
    val catalog = new HiveCatalog()
    catalog.setConf(conf)
    catalog
  }
  def resolveFiles(tableName: String): Seq[DataFile] = {
    val name = TableIdentifier.parse(tableName)
    val table = catalog.loadTable(name)
    table.newScan.planFiles.asScala.map(_.file()).toSeq
  }

  def isValid(bcastConf: Broadcast[HadoopConf], file: DataFile): Boolean = {
    val hadoopConf = bcastConf.value
    val path = new Path(String.valueOf(file.path()))
    val fs = path.getFileSystem(hadoopConf)
    if (!fs.exists(path)) {
      false
    } else {
      try {
        // ok file exists lets try and see if it's a valid file for the type
        file.format() match {
          case FileFormat.ORC =>
            val reader = OrcFile.createReader(path, OrcFile.readerOptions(hadoopConf))
            val rowReader = reader.rows()
            val numRows = reader.getNumberOfRows()
          case FileFormat.AVRO =>
            val inStream = new BufferedInputStream(fs.open(path));
            val reader = new DataFileStream(inStream, new GenericDatumReader());
            val schema = reader.getSchema()
            while (reader.hasNext()) {
              reader.next()
            }
          case FileFormat.PARQUET =>
            val reader = ParquetFileReader.open(hadoopConf, path)
            val meta = reader.getFileMetaData()
          case _ =>
            // We don't have any fancy checks for this type.
        }
        true
      } catch {
        case e: Exception => false
      }
    }
  }


  def selectFilesForRemoval(tableName: String): Seq[DataFile] = {
    val sc = spark.sparkContext
    var candidates = resolveFiles(tableName).toSet
    var filesRDD = sc.parallelize(candidates.toSeq)
    val hadoopConf = sc.hadoopConfiguration
    val bcastConf = sc.broadcast(hadoopConf)
    val toRemove = ArrayBuilder.make[DataFile]
    toRemove ++= (filesRDD.filter { f => isValid(bcastConf, f) }.collect())
    // Iceberg tables can get written to a lot, lets catch up on any new files
    var newFiles = resolveFiles(tableName).toSet -- candidates
    while (newFiles.size > 0) {
      candidates ++= newFiles
      filesRDD = sc.parallelize(newFiles.toSeq)
      toRemove ++= (filesRDD.filter { f => isValid(bcastConf, f) }.collect())
    }
    toRemove.result()
  }
  def cleanTable(tableName: String): Unit = {

  }
}

/**
 * Entry point for Spark Submit etc.
 */
object IcebergTableCleaner {
  def main(args: Array[String]): Unit = {

  }
}
