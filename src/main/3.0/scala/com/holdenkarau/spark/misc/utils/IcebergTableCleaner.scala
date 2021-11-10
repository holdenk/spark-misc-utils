package com.holdenkarau.spark.misc.utils

import org.apache.iceberg.spark.SparkCatalog
import org.apache.iceberg.Table
import org.apache.spark.sql._
import org.apache.spark.sql.connector.catalog.Identifier

class IcebergTableCleaner(spark: SparkSession) extends IcebergTableCleanerBase(spark) {
  def tableForName(catalogName: String, tableName: String): Table = {
    val catalog = spark.sessionState.catalogManager.catalog(catalogName).asInstanceOf[SparkCatalog]
    val parts = tableName.split("\\.")
    val name = Identifier.of(parts.init.toArray, parts.last)
    catalog.loadTable(name).table()
  }
  def cleanTable(catalogName: String, tableName: String): Unit = {
    val table = tableForName(catalogName, tableName)
    cleanTable(table)
  }
}
