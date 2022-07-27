package com.edtech.dimensiontable

import com.edtech.Job
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class UserDetails extends Job {

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

    val dimUserDetails =
      spark.read.format("bigquery")
        .option("table", "dimension_tables.dim_user_details_1")
        .load()

    val readDf = spark.read.format("csv").schema(user_details_schema)
      .option("header", "true")
      //.option("path", "gs://edtech_analytics_dump/data/user_details/user_details2.csv")
      .option("path", "gs://edtech_analytics_dump/data/user_details/part-00000-46b05944-60ca-4939-995c-3c945240dc09-c000.csv")
      .load()
      .cache()

    readDf.createOrReplaceTempView("userdetails")
    dimUserDetails.createOrReplaceTempView("dim_user_details")

    /**
     * Check if dim_user_Details has data already
     */
    val countDf = dimUserDetails.count()
    if (countDf > 0) {

      val transformedDf = spark.sql(
        """Select concat(user_id,'-',user_name)  as  user_details_key, userid, user_name, user_email
              from userdetails where concat(user_id,'-',user_name) not in(select user_details_key from dim_user_details)
           """)

      transformedDf.show()

      transformedDf.write.format("bigquery")
        .option("table", "dimension_tables.dim_user_details_1")
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
        .option("table", "dimension_tables.dim_user_details_1")
        .mode(SaveMode.Append)
        .save()

    }
  }

}