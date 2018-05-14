package com.thoughtworks.ca.de.batch.citibike

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CitibikeTransformer {
  def main(args: Array[String]): Unit = {
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val spark =
      SparkSession.builder.appName("Citibike Pipeline: Transform").getOrCreate()
    log.info("Citibike Application Initialized: " + spark.sparkContext.appName)

    if (args.length < 2) {
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
      .withColumn("delta_lat", radians(col("end_station_latitude")) - radians(col("start_station_latitude")))
      .withColumn("delta_long", radians(col("end_station_longitude")) - radians(col("end_station_longitude")))
      .withColumn("arch", sin(col("delta_lat") / 2) * sin(col("delta_lat") / 2) +
        cos(radians(col("start_station_latitude"))) * cos(radians(col("end_station_latitude"))) *
          sin(col("delta_long") / 2) * sin(col("delta_long") / 2))
      .withColumn("curve", atan2(sqrt(col("arch")), sqrt(-col("arch") + 1)) * 2)
      .withColumn("distance_miles", col("curve") * 6371000 / 1609.34)
      .withColumn("distance", round(col("distance_miles"), 2))
      .drop("delta_lat", "delta_long", "arch", "curve", "distance_miles")
      .write
      .parquet(outputPath)
  }
}
