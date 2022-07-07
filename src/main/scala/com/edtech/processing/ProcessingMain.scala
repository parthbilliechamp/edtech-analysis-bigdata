package com.edtech.processing

import org.apache.spark.sql.SparkSession

import java.util.Properties

/**
 * Entry point of the spark job
 */
object ProcessingMain {

  //loading properties from the property file
  val props = new Properties()
  props.load(classOf[App].getClassLoader.getResourceAsStream("application.properties"))
  val MASTER_CONFIG: String = props.getProperty("master_config")

  //creating spark session object
  val spark: SparkSession = SparkSessionFactory.getOrCreateSparkSessionObject(MASTER_CONFIG)



}
