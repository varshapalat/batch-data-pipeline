package com.thoughtworks.ca.de.transform

import java.text.SimpleDateFormat
import java.util.Calendar

import com.thoughtworks.ca.de.utils.DateUtils
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession

object DailyDriver {
  def main(args: Array[String]) {
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

    //Read ingested data
    log.info("Reading ingested data...")
    val flightData = spark.read.parquet(
      conf
        .getString("transform.input.hdfs.url")
        .format(conf.getString("common.hdfs.host"),
                conf.getString("common.hdfs.lake1Path"),
                conf.getString("tranform.input.hdfs.dateSetId"),
                processingDate)
    )
    log.info("Reading ingested data done")

    log.info("Describe flight data")
    flightData.printSchema()

    //Example cleanup: replace all null or empty values with 0
    flightData.na.replace(flightData.columns, Map("" -> "0")).show()

    //Save flight data to lake 2
    log.info("Writing data to lake 2...")
    flightData.write.parquet(
      conf
        .getString("ingest.output.hdfs.host")
        .format(conf.getString("common.hdfs.host"),
                conf.getString("common.hdfs.lake2Path"),
                conf.getString("transform.output.hdfs.dateSetId"),
                processingDate)
    )
    log.info("Writing data to lake 2 done")

    log.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
  }
}
