package be.dataminded.nmbsanalytics.cleansing

import java.nio.file.Paths

import be.dataminded.nmbsanalytics.sources.GtfsRealtimeSource
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.parallel.ForkJoinTaskSupport

/**
  * Created by krispeeters on 02/08/2017.
  */
object CleansingGtfsRealtime {
  def run(protobufPath: String, output: String): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName("NMBS Analytics")
      .master("local[4]")
      .getOrCreate()


    val years = List(2017)
    val months = List.range(1, 13)
    val days = List.range(1, 31).par
    days.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(5))
    years.foreach(year =>
      months.foreach(month =>
        days.foreach { day =>
          val yearString = "%02d".format(year)
          val monthString = "%02d".format(month)
          val dayString = "%02d".format(day)
          val folder = Paths.get(protobufPath, yearString, monthString, dayString).toFile
          if (folder.exists()) {
            val outputFolder = Paths.get(output, yearString, monthString, dayString).toFile
            println(s"start loading ${folder.toString} into ${outputFolder.toString}")
            val messages = GtfsRealtimeSource.loadRaw(spark, folder.toString)
            messages.write.mode(SaveMode.Overwrite).parquet(outputFolder.toString)
            println(s"done loading ${folder.toString} into ${outputFolder.toString}")
          }
        }
      )
    )

  }
}
