package be.dataminded.nmbsanalytics.delays

import be.dataminded.nmbsanalytics.sources.{Delay, GtfsRealtimeSource}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
  * Created by krispeeters on 02/08/2017.
  */
object CalculateDelays {
  def run(spark: SparkSession, gtfsRealtimePath: String, output: String): Unit = {
    val realtimeDelays = GtfsRealtimeSource.load(spark, gtfsRealtimePath)
    val delays = calculateDelays(spark, realtimeDelays)
    delays.coalesce(1).write.mode(SaveMode.Overwrite).parquet(output)
  }

  def calculateDelays(spark: SparkSession, realtimeDelays: Dataset[Delay]): Dataset[Delay] = {
    import spark.implicits._

    val actualDelays = realtimeDelays
      .groupByKey(_.key)
      .mapGroups {
        (key, delays) =>
          val sortedDelays = delays.toList.sortBy(_.path)
          sortedDelays.last
      }
    actualDelays
  }
}
