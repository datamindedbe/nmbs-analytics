package be.dataminded.nmbsanalytics.jobs

import be.dataminded.nmbsanalytics.delays.CalculateDelays
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CalculateDelaysApplication extends App {
  val realtimePath = "/Users/krispeeters/data/nmbs/delays/nmbs/cleansed/realtime/2017/*/*"
  //val feedPath = "/Users/krispeeters/data/nmbs/delays/nmbs/cleansed/feed"
  val output = "/Users/krispeeters/data/nmbs/delays/nmbs/master/delays"

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  val spark = SparkSession
    .builder
    .appName("NMBS Delays")
    .master("local[4]")
    .getOrCreate()

  CalculateDelays.run(spark, realtimePath, output)
}


