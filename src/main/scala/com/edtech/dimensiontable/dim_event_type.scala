package com.edtech.dimensiontable

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField

object dim_event_type extends App{
  
  
  Logger.getLogger("org").setLevel(Level.ERROR);
  
   val spark = SparkSession.builder().master("local[*]")
    .appName("populatingDimMarketingTeam")
    .getOrCreate()
    
      
    val bucket = "edtech_analytics_dump"
  spark.conf.set("temporaryGcsBucket", bucket)

 
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
   val df1 = (spark.read.format("csv")
      .schema(campaign_details_Schema)
    .option("header", true)
    .option("path", "gs://edtech_analytics_dump/data/bq-results.csv")
    .load())
    
    
    df1.createOrReplaceTempView("userEventDetails");
   
   /**
    * Populating dim marketing teams details
    */
   val dimEventType = (spark.read.format("bigquery")
                           .option("table", "dimension_tables.dim_event_type")
                            .load())
                            
    dimEventType.createOrReplaceTempView("dim_event_type")
    
    
    val eventCount = dimEventType.count();
   if(eventCount >0){
     
     
   }else{
     
     
     
   }

  
}