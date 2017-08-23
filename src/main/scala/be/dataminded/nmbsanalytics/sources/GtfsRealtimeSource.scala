package be.dataminded.nmbsanalytics.sources

import com.google.transit.realtime.GtfsRealtime.FeedMessage
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable.ListBuffer

case class Delay(
                  key: String,
                  tripId: String,
                  startTime: String,
                  startDate: String,
                  stopId: String,
                  departureDelay: Long,
                  arrivalDelay: Long,
                  path: String)

object GtfsRealtimeSource {
  def loadRaw(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._
    val protobufFiles: RDD[(String, PortableDataStream)] = spark.sparkContext.binaryFiles(path)
    val messages: RDD[Delay] = protobufFiles.flatMap {
      case (name: String, stream: PortableDataStream) =>
        val feed: FeedMessage = FeedMessage.parseFrom(stream.open())
        val delayUpdates = ListBuffer[Delay]()
        import scala.collection.JavaConversions._
        for (entity <- feed.getEntityList) {
          if (entity.hasTripUpdate) {
            val tripUpdate = entity.getTripUpdate
            for (update <- tripUpdate.getStopTimeUpdateList) {
              val delay = Delay(
                key = tripUpdate.getTrip.getTripId + "||" +
                  tripUpdate.getTrip.getStartDate + "||" +
                  tripUpdate.getTrip.getStartTime + "||" +
                  update.getStopId,
                tripId = tripUpdate.getTrip.getTripId,
                startDate = tripUpdate.getTrip.getStartDate,
                startTime = tripUpdate.getTrip.getStartTime,
                stopId = update.getStopId,
                departureDelay = update.getDeparture.getDelay,
                arrivalDelay = update.getArrival.getDelay,
                path = name
              )
              delayUpdates += delay
            }
          }
        }
        delayUpdates.toList
    }
    val result = messages.toDF()
    result
  }

  def load(spark: SparkSession, path: String): Dataset[Delay] = {
    import spark.implicits._
    spark.read.parquet(path).as[Delay]
  }
}
