package com.thoughtworks.ca.de.batch.transit

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession

object TransformDailyDriver {
  def main(args: Array[String]) {
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val spark =
      SparkSession.builder.appName("Skinny Pipeline: Transform").getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)

    //Parse argument/s
    if (args.length < 2) {
      spark.stop()
      log.warn("Input source and output path are required")
      System.exit(1)
    }
    val ingestPath = args(0)
    val transformationPath = args(1)

    run(spark, ingestPath, transformationPath)

    log.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
  }

  def run(sparkSession: SparkSession,
          ingestPath: String,
          transformationPath: String): Unit = {
    sparkSession
      .read
      .parquet(ingestPath)
      .write
      .parquet(transformationPath)
  }
}
