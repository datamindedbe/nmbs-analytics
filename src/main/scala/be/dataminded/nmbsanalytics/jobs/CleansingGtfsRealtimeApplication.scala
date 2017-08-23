package be.dataminded.nmbsanalytics.jobs

import be.dataminded.nmbsanalytics.cleansing.CleansingGtfsRealtime

object CleansingGtfsRealtimeApplication extends App {
  val path = "/Users/krispeeters/data/nmbs/delays/nmbs/realtime"
  val output = "/Users/krispeeters/data/nmbs/delays/nmbs/cleansed/"
  CleansingGtfsRealtime.run(path, output)
}


