package com.holdenkarau.spark.misc.utils

import java.io.BufferedInputStream

import collection.JavaConverters._
import collection.mutable.{ArrayBuilder, HashSet}

import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.GenericDatumReader
import org.apache.hadoop.conf.{Configuration => HadoopConf}
import org.apache.hadoop.fs._
import org.apache.iceberg.BaseMetastoreCatalog
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.hive.HiveCatalog
import org.apache.iceberg.spark.SparkCatalog
import org.apache.iceberg._
import org.apache.iceberg.catalog._
import org.apache.orc.OrcFile
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.util.SerializableConfiguration


/**
 * Class so we can test our functions
 */
class IcebergTableCleaner(spark: SparkSession, catalogName: String) {
  val sc = spark.sparkContext

  def tableForName(catalogName: String, tableName: String): Table = {
    val catalog = spark.sessionState.catalogManager.catalog(catalogName).asInstanceOf[SparkCatalog]
    val parts = tableName.split("\\.")
    val name = Identifier.of(parts.init.toArray, parts.last)
    catalog.loadTable(name).table()
  }

  def resolveFiles(table: Table): Seq[DataFile] = {
    table.newScan.planFiles.asScala.map(_.file()).toSeq
  }

  def selectFilesForRemoval(table: Table): Seq[(DataFile, String)] = {
    var candidateFiles = HashSet[String]()
    val hadoopConf = sc.hadoopConfiguration
    val bcastConf = sc.broadcast(new SerializableConfiguration(hadoopConf))
    val toRemove = ArrayBuilder.make[(DataFile, String)]
    // Iceberg tables can get written to a lot, lets catch up on any new files
    var newFiles = resolveFiles(table).filter(f => !candidateFiles.contains(f.path.toString ))
    while (newFiles.size > 0) {
      candidateFiles ++= newFiles.map(_.path.toString)
      val filesRDD = sc.parallelize(newFiles.toSeq)
      toRemove ++= (filesRDD.flatMap { f => IcebergTableCleaner.validate(bcastConf, f) }.collect())
      newFiles = resolveFiles(table).filter(f => !candidateFiles.contains(f.path.toString))
    }
    toRemove.result()
  }

  def cleanTable(catalogName: String, tableName: String): Unit = {
    val table = tableForName(catalogName, tableName)
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
  def validate(bcastConf: Broadcast[SerializableConfiguration], file: DataFile): Option[(DataFile, String)] = {
    val hadoopConf = bcastConf.value.value
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

  def main(args: Array[String]): Unit = {

  }
}
