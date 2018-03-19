package com.thoughtworks.ca.de.batch.app

/*
This app generates count of Uber rides in NYC by Humidity Range
Input data frames: uber rides & weather
Dependency: Upstream ingest and tranform processes
 */

import com.thoughtworks.ca.de.common.utils.{ConfigUtils, DateUtils}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_date

object UberRidesByHumidityRange {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val spark =
      SparkSession.builder.appName("Skinny Pipeline: Transform").getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)

    //Parse argument/s
    var processingDate = DateUtils.date2TWFormat()
    if (!args.isEmpty) {
      processingDate = DateUtils.parseISO2TWFormat(args(0))
    }

    val uberData = spark.read.parquet(
      conf
        .getString("apps.uri")
        .format(conf.getString("common.hdfs.lake2Path"),
                conf.getString("apps.UberRidesByHumidityRange.uberData"),
                processingDate))
    val weatherData = spark.read.parquet(
      conf
        .getString("apps.uri")
        .format(conf.getString("common.hdfs.lake2Path"),
                conf.getString("apps.UberRidesByHumidityRange.weatherData"),
                processingDate))

    val humidityRangeCount = uberData
      .join(weatherData, uberData("DATE") <=> weatherData("date"))
      .groupBy("humidity_range")
      .count()
    humidityRangeCount.show()

    humidityRangeCount
      .repartition(1)
      .write
      .option("header", "true")
      .csv(
        conf
          .getString("apps.uri")
          .format(conf.getString("common.hdfs.lake3Path"),
                  conf.getString("apps.UberRidesByHumidityRange.output"),
                  processingDate))

    log.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
  }
}
