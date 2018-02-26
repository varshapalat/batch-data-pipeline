package com.thoughtworks.ca.de.egress

import java.text.SimpleDateFormat
import java.util.Calendar

import com.datastax.spark.connector._
import com.datastax.spark.connector.SomeColumns
import com.thoughtworks.ca.de.utils.DateUtils
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession

class DailyDriver {
  def main(args: Array[String]) {
    val conf = ConfigFactory.load
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val spark =
      SparkSession.builder.appName("Skinny Pipeline: Egress").getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)

    //Parse argument/s
    var processingDate = DateUtils.date2TWFormat()
    if (!args.isEmpty) {
      processingDate = DateUtils.parseISO2TWFormat(args(0))
    }

    //Set Cassandra configurations
    log.info("Intializing Cassandra Credentials...")
    spark.conf.set("spark.cassandra.connection.host",
                   conf.getString("egress.output.cassandra.host"))
    spark.conf.set("spark.cassandra.auth.username",
                   conf.getString("egress.output.cassandra.username"))
    spark.conf.set("spark.cassandra.auth.password",
                   conf.getString("egress.output.cassandra.password"))
    log.info("Intializing Cassandra Credentials done")

    //Read transformed data
    log.info("Reading transformed data...")
    val flightData = spark.read.parquet(
      conf
        .getString("egress.input.hdfs.url")
        .format(
          conf.getString("common.hdfs.host"),
          conf.getString("common.hdfs.lake2Path"),
          conf.getString("egress.input.hdfs.dateSetId"),
          processingDate
        ))
    log.info("Reading transformed data done")

    log.info("Describe flight data")
    flightData.printSchema()

    //Save flight data to datamart (Cassandra)
    log.info("Writing data to datamart (Cassandra)...")
    flightData.rdd.saveToCassandra(
      conf.getString("egress.output.cassandra.keySpaceName"),
      conf.getString("egress.output.cassandra.tableName"),
      SomeColumns("key", "value"))
    log.info("Writing data to datamart (Cassandra) Done")

    log.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
  }
}
