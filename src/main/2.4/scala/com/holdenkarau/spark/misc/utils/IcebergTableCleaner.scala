package com.holdenkarau.spark.misc.utils

import org.apache.spark.sql._
import org.apache.iceberg.Table
import org.apache.iceberg.spark.source._
import org.apache.iceberg.catalog._

class IcebergTableCleaner(spark: SparkSession) extends IcebergTableCleanerBase(spark) {
  def tableForName(catalogName: String, tableName: String): Table = {
    val catalog = CustomCatalogs.loadCatalog(spark, catalogName)
    val name = TableIdentifier.of(tableName)
    catalog.loadTable(name)
  }
  def cleanTable(catalogName: String, tableName: String): Unit = {
    val table = tableForName(catalogName, tableName)
    cleanTable(table)
  }
}
