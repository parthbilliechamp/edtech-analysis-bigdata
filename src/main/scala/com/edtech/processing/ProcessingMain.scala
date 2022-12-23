package com.edtech.processing

import com.edtech.Job
import com.edtech.dimensiontable.{CampaignDimensions, CountryType, DeviceType, EventType, MarketingTeam, UserDetails, UserDimensionCountry}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

/**
 * Entry point of the spark job
 */
object ProcessingMain extends App {

  val logger = Logger.getLogger("ProcessingMain")

  val jobList: List[Job] = List(new CampaignDimensions,
    new CountryType,
    new DeviceType,
    new EventType,
    new MarketingTeam,
    new UserDetails,
    new UserDimensionCountry)

  //creating spark session object
  val spark: SparkSession = SparkSessionFactory.getOrCreateSparkSessionObject("yarn")

  logger.info("Spark processing started")
  jobList.foreach(x => x.execute(spark))
  logger.info("Spark processing completed")

}
