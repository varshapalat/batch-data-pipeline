package com.thoughtworks.ca.de.batch.suicides

import java.time.LocalDateTime

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

case class SuicideData(dataFrame: DataFrame) {

  def totalSuicidesInYearByGender(inYear: Integer): DataFrame = {
    dataFrame.filter(col("year") === inYear)
      .groupBy(col("sex"))
      .agg(sum("suicides_no"))
  }
}

object SuicideData {
  val log: Logger = LogManager.getRootLogger

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Suicide Count").getOrCreate()
    log.setLevel(Level.INFO)
    log.info("Application Initialized: " + spark.sparkContext.appName)

    val inputPath = if(!args.isEmpty) args(0) else "../sample-data/Suicide Rates Overview 1985 to 2016.csv"
    val outputPath = if(args.length > 1) args(1) else "./target/test-" + LocalDateTime.now()

    run(inputPath, outputPath)

    log.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
  }

  def run( inputPath: String, outputPath: String): Unit = {

    val spark = SparkSession.getActiveSession.get
    val suicideDF = spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv(inputPath)

    println("The total number of suicides in year 1991 by gender: ")
    SuicideData(suicideDF).totalSuicidesInYearByGender(1991).show()

  }
}
