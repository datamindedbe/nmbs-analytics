package be.dataminded.nmbsanalytics

import be.dataminded.nmbsanalytics.sources.GtfsFeedSource
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, Matchers}

class GtfsFeedTest extends FunSuite with Matchers {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  val spark = SparkSession
    .builder
    //.config("spark.driver.host", "localhost")
    .appName("NMBS Analytics")
    .master("local[2]")
    .getOrCreate()

  val path = "src/test/resources/gtfs"

  test("Protobufs are read correctly") {
    val feed = GtfsFeedSource.load(spark, path)

    assert(feed.agencies.count() == 1)
    assert(feed.calendar.count() == 4234)
    assert(feed.calendarDates.count() == 414295)
    assert(feed.routes.count() == 466)
    assert(feed.stops.count() == 2598)
    assert(feed.stopTimeOverrides.count() == 114844)
    assert(feed.transfers.count() == 605)
    assert(feed.translations.count() == 2236)
    assert(feed.trips.count() == 13441)
  }
}
