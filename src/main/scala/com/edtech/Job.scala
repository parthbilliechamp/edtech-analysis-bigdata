package com.edtech

import org.apache.spark.sql.SparkSession

trait Job {

  def execute(spark: SparkSession) : Unit

}
