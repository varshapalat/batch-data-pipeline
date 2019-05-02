package com.thoughtworks.ca.de.batch.suicides

import java.time.LocalDateTime

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession

object SuicideDataTransformer {
  val log: Logger = LogManager.getRootLogger

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Suicide Count").getOrCreate()
    log.setLevel(Level.INFO)
    log.info("Application Initialized: " + spark.sparkContext.appName)

    val inputPath = if(!args.isEmpty) args(0) else "../sample-data/Suicide Rates Overview 1985 to 2016.csv"
    val outputPath = if(args.length > 1) args(1) else "./target/test-" + LocalDateTime.now()

    run(spark, inputPath, outputPath)

    log.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
  }

  def run(spark: SparkSession, inputPath: String, outputPath: String): Unit = {

    val suicideDF = spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv(inputPath)

    import com.thoughtworks.ca.de.batch.suicides.SuicideUtils._

    println("The total number of suicides in year 1991 by gender: ")
    suicideDF.totalSuicidesInYearByGender(spark, 1991).show()

  }
}
