package com.edtech.dimensiontable

import com.edtech.Job
import org.apache.spark.sql.SparkSession

class CountryType extends Job  {

  def execute(spark: SparkSession) : Unit = {

    import org.apache.spark.sql.SaveMode
    import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

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

    val dimCountryType =
      (spark.read.format("bigquery")
        .option("table", "dimension_tables.dim_country_1")
        .load())

    val readDf = (spark.read.format("csv").schema(user_details_schema)
      .option("header", "true")
      //.option("path", "gs://edtech_analytics_dump/data/user_details/user_details2.csv")
      .option("path", "gs://edtech_analytics_dump/data/user_details/part-00000-46b05944-60ca-4939-995c-3c945240dc09-c000.csv")
      .load()
      .cache())

    readDf.createOrReplaceTempView("userdetails")
    dimCountryType.createOrReplaceTempView("dim_country")

    /**
     * Check if dim_country has data already
     */
    val countDf = dimCountryType.count();
    if (countDf > 0) {

      val transformedDf = spark.sql(
        """Select concat(user_country,'-',user_state)  as  user, user_country, user_state, user_timezone
              from userdetails where concat(user_country,'-',user_state) not in(select user from dim_country)
           """)

      transformedDf.show()

      (transformedDf.write.format("bigquery")
        .option("table", "dimension_tables.dim_country_1")
        .mode(SaveMode.Append)
        .save())

    } else {

      /**
       * Add the distinct user country to dimension table
       */
      val readDf1 = readDf.groupBy("user_country", "user_state", "user_timezone").count();

      val transformedDf = readDf1.selectExpr("concat(user_country,'-',user_state) as user", "user_country", "user_state", "user_timezone")

      transformedDf.show()

      (transformedDf.write.format("bigquery")
        .option("table", "dimension_tables.dim_country_1")
        .mode(SaveMode.Append)
        .save())

    }
  }

  
}