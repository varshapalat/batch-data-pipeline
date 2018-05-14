package com.thoughtworks.ca.de.batch.citibike

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._

object CitibikeTransformer {
  def main(args: Array[String]) {
    val conf = ConfigFactory.load
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val spark =
      SparkSession.builder.appName("Citibike Pipeline: Transform").getOrCreate()
    log.info("Citibike Application Initialized: " + spark.sparkContext.appName)

    if (args.size < 2) {
      spark.stop()
      log.warn("Input source and output path are required")
      System.exit(1)
    }

    val ingestPath = args(0)
    val transformationPath = args(1)

    run(spark, ingestPath, transformationPath)

    log.info("Citibike Application Done: " + spark.sparkContext.appName)
    spark.stop()
  }


  def run(sparkSession: SparkSession, ingestPath: String, outputPath: String): Unit = {
    sparkSession
      .read
      .parquet(ingestPath)
      .withColumn("distance", round(atan2(sqrt(sin((radians(col("end_station_latitude")) - radians(col("start_station_latitude"))) / 2) * sin((radians(col("end_station_latitude")) - radians(col("start_station_latitude"))) / 2) +
        cos(radians(col("start_station_latitude"))) * cos(radians(col("end_station_latitude"))) *
          sin((radians(col("end_station_longitude")) - radians(col("end_station_longitude"))) / 2) * sin((radians(col("end_station_longitude")) - radians(col("end_station_longitude"))) / 2)), sqrt((sin((radians(col("end_station_latitude")) - radians(col("start_station_latitude"))) / 2) * sin((radians(col("end_station_latitude")) - radians(col("start_station_latitude"))) / 2) +
        cos(radians(col("start_station_latitude"))) * cos(radians(col("end_station_latitude"))) *
          sin((radians(col("end_station_longitude")) - radians(col("end_station_longitude"))) / 2) * sin((radians(col("end_station_longitude")) - radians(col("end_station_longitude"))) / 2)) * -1 + 1)) * 2 * 6371000 / 1609.34 , 2))
      .write
      .parquet(outputPath)
  }


}
