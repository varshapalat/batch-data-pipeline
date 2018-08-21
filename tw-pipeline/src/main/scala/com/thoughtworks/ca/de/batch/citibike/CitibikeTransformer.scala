package com.thoughtworks.ca.de.batch.citibike

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession

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

  def run(sparkSession: SparkSession,
          ingestPath: String,
          outputPath: String): Unit = {

    import com.thoughtworks.ca.de.batch.citibike.CitibikeTransformerUtils._

    sparkSession.read
      .parquet(ingestPath)
      .computeDistances(sparkSession)
      .write
      .parquet(outputPath)
  }
}
