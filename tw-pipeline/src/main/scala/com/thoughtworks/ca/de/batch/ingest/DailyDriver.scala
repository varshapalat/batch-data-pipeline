package com.thoughtworks.ca.de.batch.ingest

import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory

import com.thoughtworks.ca.de.common.utils.{
  DataframeUtils
}
import org.apache.log4j.{Level, LogManager}

object DailyDriver {
  def main(args: Array[String]) {
    val conf = ConfigFactory.load
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val spark =
      SparkSession.builder.appName("Skinny Pipeline: Ingest").getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)

    //Parse argument/s
    if (args.size < 2) {
      spark.stop()
      log.warn("Input source and output path are required")
      System.exit(1)
    }
    val inputSource = args(0)
    val outputPath = args(1)

    DataframeUtils.formatColumnHeaders(spark.read
      .format("org.apache.spark.csv")
      .option("header", true)
      .csv(inputSource))
      .write
      .parquet(outputPath)


    log.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
  }
}
