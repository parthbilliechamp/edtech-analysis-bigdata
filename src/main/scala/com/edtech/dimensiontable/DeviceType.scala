package com.edtech.dimensiontable

import com.edtech.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.SaveMode

class DeviceType extends Job {

  def execute(spark: SparkSession): Unit = {

    val user_details_schema = StructType(List(
      StructField("user_id", IntegerType),
      StructField("user_name", StringType),
      StructField("user_email", StringType),
      StructField("user_country", StringType),
      StructField("user_state", StringType),
      StructField("user_timezone", StringType),
      StructField("user_last_activity", IntegerType),
      StructField("user_device_type", StringType)));

    val bucket = "edtech_analytics_dump"
    spark.conf.set("temporaryGcsBucket", bucket)

    val dimDeviceType =
      (spark.read.format("bigquery")
        .option("table", "dimension_tables.dim_device_type")
        .load())

    val readDf = (spark.read.format("csv").schema(user_details_schema)
      .option("header", "true")
      //.option("path", "gs://edtech_analytics_dump/data/user_details/user_details2.csv")
      .option("path", "gs://edtech_analytics_dump/data/user_details/part-00000-46b05944-60ca-4939-995c-3c945240dc09-c000.csv")
      .load()
      .cache())

    readDf.createOrReplaceTempView("userdetails")
    dimDeviceType.createOrReplaceTempView("dim_device_type")

    /**
     * Check if dim_Device_type has data already
     */
    val countDf = dimDeviceType.count();
    if (countDf > 0) {
      /**
       * Check for the user device type which is not listed dim_Device_type and add it to the dimension table
       */
      val dimDeviceTypeDf1 = spark.sql(
        """select
               (Select max(device_key) from dim_device_type)+row_number() over(order by user_device_type) as device_key, user_device_type as device_type
              from userdetails where user_device_type not in(select device_type from dim_device_type) group by user_device_type
           """)

      dimDeviceTypeDf1.show()

      (dimDeviceTypeDf1.write.format("bigquery")
        .option("table", "dimension_tables.dim_device_type")
        .mode(SaveMode.Append)
        .save())

    } else {

      /**
       * Add the distinct user device types to dimension table
       */

      val dimDeviceTypeDf1 = spark.sql(
        """select
            row_number() over(order by user_device_type) as device_key, user_device_type as device_type
              from userdetails where user_device_type not in(select device_type from dim_device_type) group by user_device_type
           """)

      dimDeviceTypeDf1.show()

      (dimDeviceTypeDf1.write.format("bigquery")
        .option("table", "dimension_tables.dim_device_type")
        .mode(SaveMode.Append)
        .save())

    }
  }

}