package com.thoughtworks.ca.de.batch.citibike

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
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

  private final val MetersPerFoot = 0.3048
  private final val FeetPerMile = 5280

  final val EarthRadiusInM: Double = 6371e3
  final val MetersPerMile: Double = MetersPerFoot * FeetPerMile

  def distanceFromLatitudeLongitudeUdf: UserDefinedFunction =
    udf((lat1: Double, lon1: Double, lat2: Double, lon2: Double) => {
      val Δlatitude = (lat2 - lat1).toRadians
      val Δlongitude = (lon2 - lon1).toRadians

      val a = Math.pow(Math.sin(Δlatitude / 2), 2) +
        Math.cos(lat1.toRadians) * Math.cos(lat2.toRadians) *
          Math.pow(Math.sin(Δlongitude / 2), 2)

      val radianDistance = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))

      val distanceInMeters = EarthRadiusInM * radianDistance
      val distanceInMiles = distanceInMeters / MetersPerMile

      distanceInMiles
    })

  def run(sparkSession: SparkSession,
          ingestPath: String,
          outputPath: String): Unit = {
    sparkSession.read
      .parquet(ingestPath)
      .withColumn(
        "distance",
        round(distanceFromLatitudeLongitudeUdf(col("start_station_latitude"),
          col("start_station_longitude"),
          col("end_station_latitude"),
          col("end_station_longitude")),
          scale = 2)
      )
      .write
      .parquet(outputPath)
  }
}
