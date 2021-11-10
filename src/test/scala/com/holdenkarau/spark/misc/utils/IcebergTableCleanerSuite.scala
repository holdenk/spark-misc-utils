package com.holdenkarau.spark.misc.utils

import com.holdenkarau.spark.testing._
import org.scalatest.FunSuite
import org.scalacheck.Arbitrary
import org.scalacheck.Prop.forAll
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
import org.apache.spark.sql._
import java.nio.file.Files
import org.apache.iceberg.DataFiles


class IcebergTableCleanerSuite extends FunSuite with SharedSparkContext with Checkers {

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
    try {
      sparkSession.sql(s"CREATE TABLE ${tblName} (id bigint, data string) USING iceberg")
    } catch {
      case e: java.lang.ClassNotFoundException => false
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
    val cleaner = new IcebergTableCleaner(session, "hadoop")
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
    val cleaner = new IcebergTableCleaner(session, meta)
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
    val cleaner = new IcebergTableCleaner(session, meta)
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

  test("Partially valid table test - invalid file") {
    val meta = "hadoop"
    val t = "invalidFile"
    val tblName = s"${meta}.${t}"
    val session = makeSession()
    // create the valid table
    assume(makeInitialTable(session, tblName))
    assert(session.read.format("iceberg").load(s"${tblName}.files").count() === 0)
    session.sql(s"INSERT INTO ${tblName} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    val filesAfterInsert = session.read.format("iceberg").load(s"${tblName}.files").count()
    assert(filesAfterInsert > 0)
    // create the not valid entry in the table.
    val cleaner = new IcebergTableCleaner(session, meta)
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
}
