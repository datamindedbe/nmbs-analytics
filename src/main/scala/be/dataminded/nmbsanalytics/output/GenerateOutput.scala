package be.dataminded.nmbsanalytics.output

import be.dataminded.nmbsanalytics.sources.{Delay, GtfsFeedSource, GtfsRealtimeSource}
import org.apache.spark.sql.functions.{expr, when}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
  * Created by krispeeters on 02/08/2017.
  */
object GenerateOutput {
  def run(spark: SparkSession, gtfsRealtimePath: String, gtfsFeedPath: String, output: String): Unit = {
    val delays = GtfsRealtimeSource.load(spark, gtfsRealtimePath)
    val feed = GtfsFeedSource.load(spark, gtfsFeedPath)
    val integratedDelays = integrate(
      spark,
      delays,
      feed.trips,
      feed.stopTimes,
      feed.stops,
      feed.routes,
      feed.calendarDates)
    //integratedDelays.show(100)
    integratedDelays
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("year_month")
      .option("header", "true")
      .csv(output)
  }

  def integrate(spark: SparkSession, delays: Dataset[Delay], trips: DataFrame, stopTimes: DataFrame, stops: DataFrame, routes: DataFrame, calendarDates: DataFrame): DataFrame = {
    import spark.implicits._
    val schedule = trips
      .join(stopTimes, Seq("trip_id"))
      .join(stops, Seq("stop_id"))
      .join(routes, Seq("route_id"))
      .join(calendarDates, Seq("service_id"))
      .withColumn("stop_id_string", 'stop_id.cast("String"))
      .select(
        "date",
        "trip_id",
        "stop_id",
        "stop_id_string",
        "stop_sequence",
        "route_id",
        "service_id",
        "trip_short_name",
        "route_short_name",
        "route_long_name",
        "route_type",
        "stop_name",
        "stop_lat",
        "stop_lon",
        "arrival_time",
        "departure_time"
      )

    val result = schedule
      .join(
        delays,
        schedule("trip_id") === delays("tripId")
          && schedule("stop_id_string") === delays("stopId")
          && schedule("date") === delays("startDate")
        , "LEFTc")
    //    realtimeUpdates.printSchema()
    //    schedule.printSchema()
    return result.select(
      "date",
      "trip_id",
      "stop_sequence",
      "route_id",
      "service_id",
      "trip_short_name",
      "route_short_name",
      "route_long_name",
      "route_type",
      "stop_name",
      "stop_lat",
      "stop_lon",
      "arrival_time",
      "departure_time",
      "arrivalDelay",
      "departureDelay")
      .withColumnRenamed("arrival_time", "scheduled_arrival_time")
      .withColumnRenamed("departure_time", "scheduled_departure_time")
      .withColumnRenamed("arrivalDelay", "arrival_delay")
      .withColumnRenamed("departureDelay", "departure_delay")
      .withColumn("arrival_delay_not_null", when($"arrival_delay".isNull, 0).otherwise($"arrival_delay"))
      .withColumn("departure_delay_not_null", when($"departure_delay".isNull, 0).otherwise($"departure_delay"))
      .withColumn("year_month", expr("SUBSTR(date,1,6)"))
  }
}
