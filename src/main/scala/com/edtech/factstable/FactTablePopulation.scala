package com.edtech.factstable

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField

object FactTablePopulation extends App {


  Logger.getLogger("org").setLevel(Level.ERROR);

  val spark = SparkSession.builder().master("local[*]")
    .appName("populatingDimCampaign")
    .getOrCreate()

  val bucket = "edtech_analytics_dump"
  spark.conf.set("temporaryGcsBucket", bucket)


  val user_details_schema = StructType(List(
    StructField("user_id", IntegerType),
    StructField("user_name", StringType),
    StructField("user_email", StringType),
    StructField("user_country", StringType),
    StructField("user_state", StringType),
    StructField("user_timezone", StringType),
    StructField("user_last_activity", IntegerType),
    StructField("user_device_type", StringType)));

  val userDetails =
    (spark.read.format("csv").schema(user_details_schema)
      .option("header", "true")
      //.option("path", "gs://edtech_analytics_dump/data/user_details/user_details2.csv")
      .option("path", "gs://edtech_analytics_dump/data/user_details/part-00000-46b05944-60ca-4939-995c-3c945240dc09-c000.csv")
      .load()
      .cache())

  val dimUserDetails = (spark.read.format("bigquery")
    .option("table", "dimension_tables.dim_user_details")
    .load())


  val dimDeviceType = (spark.read.format("bigquery")
    .option("table", "dimension_tables.dim_device_type")
    .load())

  val dimCountry = (spark.read.format("bigquery")
    .option("table", "dimension_tables.dim_country")
    .load())

  val joinType = "inner"
  val joined1 = (userDetails.join(dimUserDetails,userDetails.col("user_id")===dimUserDetails.col("user_id"),joinType)
    .join(dimDeviceType,userDetails.col("user_device_type")===dimDeviceType.col("device_type"),joinType)
    .join(dimCountry,userDetails.col("user_state")===dimCountry.col("user_state"),joinType).drop(dimUserDetails.col("user_id"))
    .drop(dimUserDetails.col("user_name")).drop(dimCountry.col("user_country")).drop(dimCountry.col("user_state")))


  val userDetailsJoinedCte = joined1.select("user_id","user_name","user_country","user_state","user_details_key","device_key","device_type","user")


  val dimCampaign = (spark.read.format("bigquery")
    .option("table", "dimension_tables.dim_Campaign")
    .load())

  val campaign_details_Schema = StructType(List(StructField("campaign_id",IntegerType),
    StructField("course_campaign_name",StringType),
    StructField("user_id",IntegerType),
    StructField("user_name",StringType),
    StructField("campaign_date",DateType),
    StructField("digital_marketing_team",StringType),
    StructField("event_status",IntegerType),
    StructField("event_type",StringType),
    StructField("marketing_product",StringType),
    StructField("user_response_time",IntegerType)));
  val userEventDetails = (spark.read.format("csv")
    .schema(campaign_details_Schema)
    .option("header", true)
    .option("path", "gs://edtech_analytics_dump/data/bq-results.csv")
    .load())

  val dimMarketingTeam = (spark.read.format("bigquery")
    .option("table", "dimension_tables.dim_marketing_team")
    .load())


  val dimEventType = (spark.read.format("bigquery")
    .option("table", "dimension_tables.dim_event_type")
    .load())

  val joined2 = (userEventDetails.join(dimCampaign,userEventDetails.col("campaign_id")===dimCampaign.col("course_campaign_id"),joinType)
    .join(dimMarketingTeam,userEventDetails.col("digital_marketing_team")===dimMarketingTeam.col("digital_marketing_team"),joinType)
    .join(dimEventType,userEventDetails.col("event_type")===dimEventType.col("eventType"),joinType)
    .drop(dimMarketingTeam.col("digital_marketing_team"))
    .drop(dimEventType.col("event_type")))


  val userEventDetailsJoined = joined2.select("campaign_key","marketing_key","event_type_key","event_type","event_status",
    "digital_marketing_team","marketing_place","course_campaign_id",
    "course_campaign_name", "course_campaign_start_date",
    "course_campaign_end_date","user_response_time","user_id")

  userEventDetails.createOrReplaceTempView("userEventDetails")


  val groupedCte = spark.sql("""SELECT campaign_id, event_type, user_id,count(*) over(partition by campaign_id,user_id,event_type) as event_count
        FROM userEventDetails""")


  userDetailsJoinedCte.join(userEventDetailsJoined,userDetailsJoinedCte.col("user_id")===userEventDetailsJoined.col("user_id"),joinType)
    .join(groupedCte,userDetailsJoinedCte.col("user_id")===groupedCte.col("user_id"))
    .drop(groupedCte.col("user_id"))
    .drop(userEventDetailsJoined.col("user_id")).createOrReplaceTempView("factTablePopulation")

  spark.sql("""select ROW_NUMBER() OVER() AS fact_campaign_key,
            campaign_key,
            user_details_key AS user_key,
            event_type_key,
            marketing_key,
            country_key,
             device_key,
            user_country,
           user_state,device_type,
           event_type, user_id,
           user_name,digital_marketing_team,
            marketing_place,
           course_campaign_id,
           course_campaign_name,
            course_campaign_start_date,
            course_campaign_end_date,
            user_response_time,
            event_status,
            event_Count,
            EXTRACT(YEAR FROM course_campaign_start_date) AS campaign_year,
            EXTRACT(MONTH FROM course_campaign_start_date) AS campaign_month,
            EXTRACT(DAY FROM course_campaign_start_date) AS campaign_date from factTablePopulation""").show



}

