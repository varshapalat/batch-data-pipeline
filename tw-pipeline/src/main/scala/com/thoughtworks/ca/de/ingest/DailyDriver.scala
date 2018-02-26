package com.thoughtworks.ca.de.ingest

import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, LogManager}

import com.thoughtworks.ca.de.utils.DateUtils

class DailyDriver {
  def main(args: Array[String]) {
    val conf = ConfigFactory.load
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val spark =
      SparkSession.builder.appName("Skinny Pipeline: Ingest").getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)

    //Parse argument/s
    var processingDate = DateUtils.date2TWFormat()
    if (!args.isEmpty) {
      processingDate = DateUtils.parseISO2TWFormat(args(0))
    }

    //Set S3 credentials
    log.info("Intializing S3 Credentials...")
    spark.sparkContext.hadoopConfiguration
      .set("fs.s3n.awsAccessKeyId", conf.getString("input.s3.awsAccessKeyId"))
    spark.sparkContext.hadoopConfiguration.set(
      "fs.s3n.awsSecretAccessKey",
      conf.getString("input.s3.awsSecretAccessKey"))
    log.info("Intializing S3 Credentials done")

    //Read flight data from S3
    log.info("Reading flight data...")
    val flightData = spark.read.parquet(
      conf
        .getString("ingest.input.s3.url")
        .format(conf.getString("ingest.input.s3.region"),
                conf.getString("ingest.input.s3.bucket"),
                conf.getString("ingest.input.s3.file")))
    log.info("Reading flight data done")

    log.info("Describe flight data")
    flightData.printSchema()

    //Save flight data to lake 1
    log.info("Writing data to lake 1...")
    flightData.write.parquet(
      conf
        .getString("ingest.output.hdfs.host")
        .format(conf.getString("common.hdfs.host"),
                conf.getString("common.hdfs.lake1Path"),
                conf.getString("ingest.output.hdfs.dateSetId"),
                processingDate)
    )
    log.info("Writing data to lake 1 done")

    log.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
  }
}
