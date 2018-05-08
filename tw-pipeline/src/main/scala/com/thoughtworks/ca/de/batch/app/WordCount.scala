package com.thoughtworks.ca.de.batch.app

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

    //Parse argument/s
    var processingDate = DateUtils.date2TWFormat
    if (!args.isEmpty) {
      processingDate = DateUtils.parseISO2TWFormat(args(0))
    }

    val inputPath = conf.getString("apps.WordCount.input")
    val outputPath = conf.getString("apps.WordCount.output").format(processingDate)

    run(spark, inputPath, outputPath)

    spark.stop()
  }

  def run(spark: SparkSession, inputPath: String, outputPath: String): Unit = {
    log.info("Reading data: " + inputPath)
    log.info("Writing data: " + outputPath)

    import spark.implicits._
    spark.read
      .text(inputPath) // Read file
      .as[String] // As a data set
      .flatMap(text => text.split("\\s+")) // Split words using regex (distributed map)
      .groupByKey(_.toLowerCase()) // Reduce action
      .count() // Aggregate
      .withColumnRenamed("count(1)", "count") // Cannot save data with column name count(1)
      .write // Write to file
      .csv(outputPath)

    log.info("Application Done: " + spark.sparkContext.appName)
  }
}
