package be.dataminded.nmbsanalytics.jobs

import be.dataminded.nmbsanalytics.output.GenerateOutput
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object GenerateOutputApplication extends App {
  val realtimePath = "/Users/krispeeters/data/nmbs/delays/nmbs/master/delays"
  val feedPath = "/Users/krispeeters/data/nmbs/delays/nmbs/cleansed/feed"
  val output = "/Users/krispeeters/data/nmbs/delays/nmbs/output/delays"

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  val spark = SparkSession
    .builder
    .config("spark.driver.host", "localhost")
    .appName("NMBS Delays")
    .master("local[4]")
    .getOrCreate()

  GenerateOutput.run(spark, realtimePath,feedPath, output)
}


