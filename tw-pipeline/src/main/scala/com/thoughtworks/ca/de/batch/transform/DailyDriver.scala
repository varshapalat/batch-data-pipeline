package com.thoughtworks.ca.de.batch.transform

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
    if(args.size<3){
      spark.stop()
      log.warn("Input source and output path are required")
      System.exit(1)
    }
    val ingestPath = args(0)
    val transformationPath = args(1)
    val datasetId = args(2)

    Transformation.transform(spark.read.parquet(ingestPath),datasetId)
        .write.parquet(transformationPath)

    log.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
  }
}
