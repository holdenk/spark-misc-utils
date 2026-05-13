package com.holdenkarau.spark.misc.utils

import java.io.BufferedInputStream

import collection.JavaConverters._
import collection.mutable.{ArrayBuilder, HashSet}

import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.GenericDatumReader
import org.apache.hadoop.fs._
import org.apache.iceberg._
import org.apache.orc.OrcFile
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._

/**
 * Class so we can test our functions
 */
class IcebergTableCleanerBase(spark: SparkSession) {
  val sc = spark.sparkContext

  def resolveFiles(table: Table): Seq[DataFile] = {
    table.newScan.planFiles.asScala.map(_.file()).toSeq
  }

  def selectFilesForRemoval(table: Table): Seq[(DataFile, String)] = {
    var candidateFiles = HashSet[(String, Long)]()
    val hadoopConf = sc.hadoopConfiguration
    val bcastConf = sc.broadcast(new SerializableConfiguration(hadoopConf))
    val toRemove = ArrayBuilder.make[(DataFile, String)]
    // Iceberg tables can get written to a lot, lets catch up on any new files
    var newFiles = resolveFiles(table)
    while (newFiles.size > 0) {
      candidateFiles ++= newFiles.map { f => (f.path.toString, f.recordCount()) }
      val filesRDD = sc.parallelize(newFiles.toSeq)
      toRemove ++= (filesRDD.flatMap { f => IcebergTableCleanerBase.validate(bcastConf, f) }.collect())
      newFiles = resolveFiles(table).filter {
        f => !candidateFiles.contains((f.path.toString, f.recordCount()))
      }
    }
    toRemove.result()
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
object IcebergTableCleanerBase {
  def validate(bcastConf: Broadcast[SerializableConfiguration], file: DataFile): Option[(DataFile, String)] = {
    val hadoopConf = bcastConf.value.value
    val path = new Path(String.valueOf(file.path()))
    val fs = path.getFileSystem(hadoopConf)
    if (!fs.exists(path)) {
      Some((file, f"${file.path()} not found"))
    } else {
      try {
        val expectedRecords = file.recordCount()
        file.format() match {
          case FileFormat.ORC =>
            val reader = OrcFile.createReader(path, OrcFile.readerOptions(hadoopConf))
            try {
              val numRows = reader.getNumberOfRows()
              if (numRows != expectedRecords) {
                Some((file, f"file row count ${numRows} did not match expected ${expectedRecords}"))
              } else {
                None
              }
            } finally {
              reader.close()
            }
          case FileFormat.AVRO =>
            val inStream = new BufferedInputStream(fs.open(path))
            try {
              val reader = new DataFileStream(inStream, new GenericDatumReader())
              try {
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
              } finally {
                reader.close()
              }
            } finally {
              inStream.close()
            }
          case FileFormat.PARQUET =>
            val reader = ParquetFileReader.open(hadoopConf, path)
            try {
              val numRows = reader.getRecordCount()
              if (numRows != expectedRecords) {
                Some((file, f"file ${numRows} did not match expected ${expectedRecords}"))
              } else {
                None
              }
            } finally {
              reader.close()
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
}
