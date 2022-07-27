package com.edtech.dimensiontable

import com.edtech.Job
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.SaveMode

class UserDimensionCountry extends Job {

  def execute(spark: SparkSession): Unit = {

    val user_details_schema = StructType(List(
      StructField("user_id", IntegerType),
      StructField("user_name", StringType),
      StructField("user_email", StringType),
      StructField("user_country", StringType),
      StructField("user_state", StringType),
      StructField("user_timezone", StringType),
      StructField("user_last_activity", IntegerType),
      StructField("user_device_type", StringType)))

    val bucket = "edtech_analytics_dump"
    spark.conf.set("temporaryGcsBucket", bucket)

    val readDf = spark.read.format("csv").schema(user_details_schema)
      .option("header", "true")
      //.option("path", "gs://edtech_analytics_dump/data/user_details/user_details2.csv")
      .option("path", "gs://edtech_analytics_dump/data/user_details/part-00000-46b05944-60ca-4939-995c-3c945240dc09-c000.csv")
      .load()
      .cache()

    readDf.createOrReplaceTempView("userdetails")


    val dimDeviceType =
      spark.read.format("bigquery")
        .option("table", "dimension_tables.dim_device_type")
        .load()


    val dimCountryType =
      spark.read.format("bigquery")
        .option("table", "dimension_tables.dim_country")
        .load()


    val dimUserDetails =
      spark.read.format("bigquery")
        .option("table", "dimension_tables.dim_user_details")
        .load()


    dimDeviceType.createOrReplaceTempView("dim_device_type")
    dimCountryType.createOrReplaceTempView("dim_country")
    dimUserDetails.createOrReplaceTempView("dim_user_details")


    /**
     * Check if dim_Device_type has data already
     */
    val dimDeviceCount = dimDeviceType.count()

    val dimCountryCount = dimCountryType.count()

    val dimUserDetailsCount = dimUserDetails.count()


    /**
     * Populate dim_device_type
     */
    if (dimDeviceCount > 0) {
      /**
       * Check for the user device type which is not listed dim_Device_type and add it to the dimension table
       */
      val dimDeviceTypeDf1 = spark.sql(
        """select
               (Select max(device_key) from dim_device_type)+row_number() over(order by user_device_type) as device_key, user_device_type as device_type
              from userdetails where user_device_type not in(select device_type from dim_device_type) group by user_device_type
           """)

      dimDeviceTypeDf1.show()

      dimDeviceTypeDf1.write.format("bigquery")
        .option("table", "dimension_tables.dim_device_type")
        .mode(SaveMode.Append)
        .save()

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

      dimDeviceTypeDf1.write.format("bigquery")
        .option("table", "dimension_tables.dim_device_type")
        .mode(SaveMode.Append)
        .save()

    }

    /**
     * Populate dim_country
     */
    if (dimCountryCount > 0) {

      val transformedDf = spark.sql(
        """Select concat(user_country,'-',user_state)  as  user, user_country, user_state, user_timezone
              from userdetails where concat(user_country,'-',user_state) not in(select user from dim_country)
           """)

      transformedDf.show()

      transformedDf.write.format("bigquery")
        .option("table", "dimension_tables.dim_country")
        .mode(SaveMode.Append)
        .save()

    } else {

      /**
       * Add the distinct user country to dimension table
       */
      val readDf1 = readDf.groupBy("user_country", "user_state", "user_timezone").count()

      val transformedDf = readDf1.selectExpr("concat(user_country,'-',user_state) as user", "user_country", "user_state", "user_timezone")

      transformedDf.show()

      transformedDf.write.format("bigquery")
        .option("table", "dimension_tables.dim_country")
        .mode(SaveMode.Append)
        .save()

    }

    /**
     * Populate UserDetails
     */
    if (dimUserDetailsCount > 0) {

      val transformedDf = spark.sql(
        """Select concat(user_id,'-',user_name)  as  user_details_key, userid, user_name, user_email
              from userdetails where concat(user_id,'-',user_name) not in(select user_details_key from dim_user_details)
           """)

      transformedDf.show()

      transformedDf.write.format("bigquery")
        .option("table", "dimension_tables.dim_user_details")
        .mode(SaveMode.Append)
        .save()

    } else {

      /**
       * Add the distinct user country to dimension table
       */
      val readDf1 = readDf.groupBy("user_id", "user_name", "user_email").count()

      val transformedDf = readDf1.selectExpr("concat(user_id,'-',user_name) as user_details_key", "user_id", "user_name", "user_email")

      transformedDf.show()

      transformedDf.write.format("bigquery")
        .option("table", "dimension_tables.dim_user_details")
        .mode(SaveMode.Append)
        .save()
    }
  }


}