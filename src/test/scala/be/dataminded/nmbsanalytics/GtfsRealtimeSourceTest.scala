package be.dataminded.nmbsanalytics

import be.dataminded.nmbsanalytics.sources.GtfsRealtimeSource
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{FunSuite, Matchers}

class GtfsRealtimeSourceTest extends FunSuite with Matchers{
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  val spark = SparkSession
    .builder
    //.config("spark.driver.host", "localhost")
    .appName("NMBS Analytics")
    .master("local[2]")
    .getOrCreate()


  test("Protobufs are read correctly") {
    val path = "src/test/resources/protobuf/sample"

    val messages = GtfsRealtimeSource.loadRaw(spark, path).cache()
    assert(messages.count() == 4518)
    messages.write.mode(SaveMode.Overwrite).parquet("target/output/delays")
  }
}
