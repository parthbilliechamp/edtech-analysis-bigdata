package com.edtech.processing.utils

import org.apache.spark.sql.DataFrame

object WriteUtil {

  def writeToCsv(dataFrame: DataFrame, outputPath: String, writeHeader: Boolean): Unit = {

    val writeHeaderInCsv = if (writeHeader) "true" else "false"
    dataFrame.coalesce(1).write
      .format("csv")
      .option("header", writeHeaderInCsv)
      .save(outputPath)
  }

}
