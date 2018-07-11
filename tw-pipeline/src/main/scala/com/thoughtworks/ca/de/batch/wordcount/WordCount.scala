package com.thoughtworks.ca.de.batch.wordcount

import java.time.Clock

import com.thoughtworks.ca.de.common.utils.DateUtils
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession

object WordCount {
  val log: Logger = LogManager.getRootLogger
  implicit val clock: Clock = Clock.systemDefaultZone()

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    log.setLevel(Level.INFO)
    val spark =
      SparkSession.builder.appName("Spark 2 Word Count").getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)

    val inputPath = if(!args.isEmpty) args(0) else conf.getString("apps.WordCount.input")
    val outputPath = if(args.length > 1) args(1) else conf.getString("apps.WordCount.output")

    run(spark, inputPath, outputPath)

    spark.stop()
  }

  def run(spark: SparkSession, inputPath: String, outputPath: String): Unit = {
    log.info("Reading data: " + inputPath)
    log.info("Writing data: " + outputPath)

    spark.read
      .text(inputPath) // add code below here to get wordcount tests to pass
      .write
      .csv(outputPath)

    log.info("Application Done: " + spark.sparkContext.appName)
  }
}
