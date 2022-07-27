package com.edtech.processing

import com.edtech.Job
import com.edtech.dimensiontable.{CampaignDimensions, CountryType, DeviceType, EventType, MarketingTeam, UserDetails, UserDimensionCountry}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

import java.util.Properties

/**
 * Entry point of the spark job
 */
object ProcessingMain extends App {

  //loading properties from the property file
  val props = new Properties()
  //props.load(classOf[App].getClassLoader.getResourceAsStream("application.properties"))
  //val MASTER_CONFIG: String = props.getProperty("master_config")

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
