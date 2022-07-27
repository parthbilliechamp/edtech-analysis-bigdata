package com.edtech.processing.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadUtil {

  def readCsvFile(spark: SparkSession, path: String, isHeaderPresent: Boolean): DataFrame = {
    val readHeader = if (isHeaderPresent) "true" else "false"
    spark.read
      .format("csv")
      .option("header", readHeader)
      .option("inferredSchema", "true")
      .option("path", path)
      .load()
  }

}