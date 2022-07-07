package com.edtech.processing

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkSessionFactory {

  var spark: SparkSession = null
  var sparkConf: SparkConf = null

  def getOrCreateSparkSessionObject(master: String): SparkSession = {
    if (null == spark) {
      spark = SparkSession.builder().config(getSparkConf(master)).getOrCreate()
    }
    spark
  }

  def getSparkConf(master: String): SparkConf = {
    if (null == sparkConf) {
      sparkConf = new SparkConf()
        .setAppName(ProcessingConstants.SPARK_APPLICATION_NAME)
        .set("deploy-mode", "client")
        .setMaster(master)
    }
    sparkConf
  }

  def getSparkContext(): SparkContext = {
    if (null == spark) {
      throw new IllegalStateException("Spark session not created!!!")
    }
    spark.sparkContext
  }

}
