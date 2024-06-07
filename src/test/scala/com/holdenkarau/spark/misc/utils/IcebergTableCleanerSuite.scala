package com.holdenkarau.spark.misc.utils

import com.holdenkarau.spark.testing._
import org.scalatest.funsuite.AnyFunSuite
import org.scalacheck.Arbitrary
import org.scalacheck.Prop.forAll
import org.scalatestplus.scalacheck.Checkers
import org.apache.spark.sql._
import java.io.File
import java.nio.file.{Files, Paths}
import org.apache.iceberg.DataFiles


// TODO: Make this suite work with Spark 2.X
class IcebergTableCleanerSuite extends AnyFunSuite with SharedSparkContext with Checkers {

  val warehouse = Files.createTempDirectory("iceberg-test")

  def makeSession(): SparkSession = {
    SparkSession.builder
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hadoop")
      .config("spark.sql.catalog.spark_catalog.warehouse", warehouse.toString)
      .config("spark.sql.catalog.hadoop", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.hadoop.type", "hadoop")
      .config("spark.sql.catalog.hadoop.warehouse", warehouse.toString)
      .getOrCreate()
  }
  // Makes the initial table, returns false so we can skip if iceberg can't be loaded.
  def makeInitialTable(sparkSession: SparkSession, tblName: String): Boolean = {
    // We use iceberg extensions for the tests, they don't really work all the way pre-3
    if (sparkSession.version < "3.0.0") {
      return false
    }
    try {
      sparkSession.sql(s"CREATE TABLE ${tblName} (id bigint, data string) USING iceberg")
    } catch {
      case e: java.lang.ClassNotFoundException => false
      //Spark 2.X errors since extensions don't support USING in default catalog
      case e: org.apache.spark.sql.AnalysisException => false
    }
    true
  }
  test("Trivial test -- load iceberg if supported.") {
    val session = makeSession()
    assume(makeInitialTable(session, "cheeseBurgers"))
  }

  test("Empty table test") {
    val tblName = "hadoop.empty"
    val session = makeSession()
    assume(makeInitialTable(session, tblName))
    val cleaner = new IcebergTableCleaner(session)
    assert(session.read.format("iceberg").load(s"${tblName}.files").count() === 0)
    val tbl = cleaner.tableForName("hadoop", "empty")
    assert(cleaner.resolveFiles(tbl).isEmpty)
    cleaner.cleanTable(tbl)
    assert(session.read.format("iceberg").load(s"${tblName}.files").count() === 0)
  }

  test("Valid table test") {
    val meta = "hadoop"
    val t = "iLike"
    val tblName = s"${meta}.${t}"
    val session = makeSession()
    assume(makeInitialTable(session, tblName))
    assert(session.read.format("iceberg").load(s"${tblName}.files").count() === 0)
    session.sql(s"INSERT INTO ${tblName} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    val filesAfterInsert = session.read.format("iceberg").load(s"${tblName}.files").count()
    assert(filesAfterInsert > 0)
    val cleaner = new IcebergTableCleaner(session)
    val tbl = cleaner.tableForName(meta, t)
    assert(!cleaner.resolveFiles(tbl).isEmpty)
    assert(cleaner.selectFilesForRemoval(tbl).isEmpty)
    cleaner.cleanTable(tbl)
    assert(session.read.format("iceberg").load(s"${tblName}.files").count() === filesAfterInsert)
  }

  test("Partially valid table test - missing file") {
    val meta = "hadoop"
    val t = "missingFile"
    val tblName = s"${meta}.${t}"
    val session = makeSession()
    // create the valid table
    assume(makeInitialTable(session, tblName))
    assert(session.read.format("iceberg").load(s"${tblName}.files").count() === 0)
    session.sql(s"INSERT INTO ${tblName} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    val filesAfterInsert = session.read.format("iceberg").load(s"${tblName}.files").count()
    assert(filesAfterInsert > 0)
    // create the not valid entry in the table.
    val cleaner = new IcebergTableCleaner(session)
    val tbl = cleaner.tableForName(meta, t)
    // Add a junk entry to the table
    val badDataFile = DataFiles.builder(tbl.spec())
      .withPath("/doesnotexist.parquet")
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
    tbl.newAppend().appendFile(badDataFile).commit();
    // Assert that our junk entry shows up.
    assert(session.read.format("iceberg").load(s"${tblName}.files").count() > filesAfterInsert)
    // Assert various things about the cleaner
    assert(!cleaner.resolveFiles(tbl).isEmpty)
    assert(!cleaner.selectFilesForRemoval(tbl).isEmpty)
    cleaner.cleanTable(tbl)
    assert(session.read.format("iceberg").load(s"${tblName}.files").count() === filesAfterInsert)
  }

  test("Partially valid table test - invalid file format") {
    val meta = "hadoop"
    val t = "invalidFileFormat"
    val tblName = s"${meta}.${t}"
    val session = makeSession()
    // create the valid table
    assume(makeInitialTable(session, tblName))
    assert(session.read.format("iceberg").load(s"${tblName}.files").count() === 0)
    session.sql(s"INSERT INTO ${tblName} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    val filesAfterInsert = session.read.format("iceberg").load(s"${tblName}.files").count()
    assert(filesAfterInsert > 0)
    // create the not valid entry in the table.
    val cleaner = new IcebergTableCleaner(session)
    val tbl = cleaner.tableForName(meta, t)
    // Create the junk file
    val badPath = warehouse.resolve(s"./${t}/data/bad.parquet")
    badPath.toFile().createNewFile()
    // Add a junk entry to the table
    val badDataFile = DataFiles.builder(tbl.spec())
      .withPath(badPath.toString)
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
    tbl.newAppend().appendFile(badDataFile).commit();
    // Assert that our junk entry shows up.
    assert(session.read.format("iceberg").load(s"${tblName}.files").count() > filesAfterInsert)
    // Assert various things about the cleaner
    assert(!cleaner.resolveFiles(tbl).isEmpty)
    val filesToRemove = cleaner.selectFilesForRemoval(tbl)
    assert(filesToRemove.size === 1)
    assert(filesToRemove(0)._2 contains "is not a Parquet file")
    cleaner.cleanTable(tbl)
    assert(session.read.format("iceberg").load(s"${tblName}.files").count() === filesAfterInsert)
  }
  // Next up test wrong record count
  test("Partially valid table test - invalid record count file") {
    val meta = "hadoop"
    val t = "invalidFileRecordCount"
    val tblName = s"${meta}.${t}"
    val session = makeSession()
    // create the valid table
    assume(makeInitialTable(session, tblName))
    assert(session.read.format("iceberg").load(s"${tblName}.files").count() === 0)
    session.sql(s"INSERT INTO ${tblName} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    val filesAfterInsert = session.read.format("iceberg").load(s"${tblName}.files").count()
    assert(filesAfterInsert > 0)
    // create the not valid entry in the table.
    val cleaner = new IcebergTableCleaner(session)
    val tbl = cleaner.tableForName(meta, t)
    // Add a junk entry to the table - make sure its in a new path since
    // Iceberg doesn't seem to support two entries to the same path.
    val oldFile = cleaner.resolveFiles(tbl)(0)
    val oldFilePath = Paths.get(oldFile.path.toString)
    val badFilePath = warehouse.resolve(s"./${t}/data/bad.parquet")
    Files.copy(oldFilePath, badFilePath)
    val badDataFile = DataFiles.builder(tbl.spec())
        .copy(oldFile)
        .withPath(badFilePath.toString)
        .withRecordCount(1000)
        .build();
    tbl.newAppend().appendFile(badDataFile).commit()
    // Assert that our junk entry shows up.
    assert(session.read.format("iceberg").load(s"${tblName}.files").count() > filesAfterInsert)
    // Assert various things about the cleaner
    assert(!cleaner.resolveFiles(tbl).isEmpty)
    val filesToRemove = cleaner.selectFilesForRemoval(tbl)
    assert(filesToRemove.size === 1)
    assert(filesToRemove(0)._2 contains "did not match expected 1000")
    cleaner.cleanTable(tbl)
    assert(session.read.format("iceberg").load(s"${tblName}.files").count() === filesAfterInsert)
  }
}
