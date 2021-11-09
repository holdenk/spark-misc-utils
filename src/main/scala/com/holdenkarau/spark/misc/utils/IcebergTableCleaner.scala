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
  val sc = spark.sparkContext

  val catalog = {
    val conf = sc.hadoopConfiguration
    val catalog = new HiveCatalog()
    catalog.setConf(conf)
    catalog
  }

  private def tableForName(tableName: String): Table = {
    val name = TableIdentifier.parse(tableName)
    catalog.loadTable(name)
  }

  def resolveFiles(table: Table): Seq[DataFile] = {
    table.newScan.planFiles.asScala.map(_.file()).toSeq
  }

  def validate(bcastConf: Broadcast[HadoopConf], file: DataFile): Option[(DataFile, String)] = {
    val hadoopConf = bcastConf.value
    val path = new Path(String.valueOf(file.path()))
    val fs = path.getFileSystem(hadoopConf)
    if (!fs.exists(path)) {
      Some((file, f"${file.path()} not found"))
    } else {
      try {
        val expectedRecords = file.recordCount()
        // todo - check and see if num records match
        // ok file exists lets try and see if it's a valid file for the type
        file.format() match {
          case FileFormat.ORC =>
            val reader = OrcFile.createReader(path, OrcFile.readerOptions(hadoopConf))
            val rowReader = reader.rows()
            val numRows = reader.getNumberOfRows()
            if (numRows != expectedRecords) {
              Some((file, f"file ${numRows} did not match expected ${expectedRecords}"))
            } else {
              None
            }
          case FileFormat.AVRO =>
            val inStream = new BufferedInputStream(fs.open(path));
            val reader = new DataFileStream(inStream, new GenericDatumReader());
            val schema = reader.getSchema()
            var numRows = 0
            while (reader.hasNext()) {
              numRows += 1
              reader.next()
            }

            if (numRows != expectedRecords) {
              Some((file, f"file ${numRows} did not match expected ${expectedRecords}"))
            } else {
              None
            }
          case FileFormat.PARQUET =>
            val reader = ParquetFileReader.open(hadoopConf, path)
            val meta = reader.getFileMetaData()
            val numRows = reader.getRecordCount()
            if (numRows != expectedRecords) {
              Some((file, f"file ${numRows} did not match expected ${expectedRecords}"))
            } else {
              None
            }
          case _ =>
            // We don't have any fancy checks for this type.
            None
        }
      } catch {
        case e: Exception =>
          Some((file, f"Exception ${e} loading datafile"))
      }
    }
  }


  def selectFilesForRemoval(table: Table): Seq[(DataFile, String)] = {
    var candidates = resolveFiles(table).toSet
    var filesRDD = sc.parallelize(candidates.toSeq)
    val hadoopConf = sc.hadoopConfiguration
    val bcastConf = sc.broadcast(hadoopConf)
    val toRemove = ArrayBuilder.make[(DataFile, String)]
    toRemove ++= (filesRDD.flatMap { f => validate(bcastConf, f) }.collect())
    // Iceberg tables can get written to a lot, lets catch up on any new files
    var newFiles = resolveFiles(table).toSet -- candidates
    while (newFiles.size > 0) {
      candidates ++= newFiles
      filesRDD = sc.parallelize(newFiles.toSeq)
      toRemove ++= (filesRDD.flatMap { f => validate(bcastConf, f) }.collect())
    }
    toRemove.result()
  }

  def cleanTable(tableName: String): Unit = {
    val table = tableForName(tableName)
    cleanTable(table)
  }

  def cleanTable(table: Table): Unit = {
    val filesToRemoveAndStatus = selectFilesForRemoval(table)
    val filesToRemove = filesToRemoveAndStatus.map(_._1)
    val op = table.newDelete()
    filesToRemove.foreach(op.deleteFile(_))
    op.commit()
  }

}

/**
 * Entry point for Spark Submit etc.
 */
object IcebergTableCleaner {
  def main(args: Array[String]): Unit = {

  }
}