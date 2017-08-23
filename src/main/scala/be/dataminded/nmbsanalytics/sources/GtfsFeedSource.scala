package be.dataminded.nmbsanalytics.sources

import org.apache.spark.sql.{DataFrame, SparkSession}

case class GtfsFeed(
                     agencies: DataFrame,
                     calendar: DataFrame,
                     calendarDates: DataFrame,
                     routes: DataFrame,
                     stopTimeOverrides: DataFrame,
                     stopTimes: DataFrame,
                     stops: DataFrame,
                     transfers: DataFrame,
                     translations: DataFrame,
                     trips: DataFrame
                   )

object GtfsFeedSource {
  def load(spark: SparkSession, path: String): GtfsFeed = {
    val result = GtfsFeed(
      agencies = spark.read.option("header", true).csv(path + "/agency.txt").cache(),
      calendar = spark.read.option("header", true).csv(path + "/calendar.txt").cache(),
      calendarDates = spark.read.option("header", true).csv(path + "/calendar_dates.txt").cache(),
      routes = spark.read.option("header", true).csv(path + "/routes.txt").cache(),
      stopTimeOverrides = spark.read.option("header", true).csv(path + "/stop_time_overrides.txt").cache(),
      stopTimes = spark.read.option("header", true).csv(path + "/stop_times.txt").cache(),
      stops = spark.read.option("header", true).csv(path + "/stops.txt").cache(),
      transfers = spark.read.option("header", true).csv(path + "/transfers.txt").cache(),
      translations = spark.read.option("header", true).csv(path + "/translations.txt").cache(),
      trips = spark.read.option("header", true).csv(path + "/trips.txt").cache()
    )
    result
  }
}
