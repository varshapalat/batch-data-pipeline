package com.thoughtworks.ca.de.batch.weather

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, date_format, to_date, udf}

object TransformDailyDriver {
  def main(args: Array[String]) {
    val conf = ConfigFactory.load
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val spark =
      SparkSession.builder.appName("Skinny Pipeline: Transform").getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)

    //Parse argument/s
    if (args.size < 3) {
      spark.stop()
      log.warn("Input source and output path are required")
      System.exit(1)
    }
    val ingestPath = args(0)
    val transformationPath = args(1)
    val datasetId = args(2)

    run(spark, ingestPath, transformationPath)

    log.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
  }

  def humidityToRangeUdf: UserDefinedFunction = udf((humidity: Int) => ((humidity + 5) / 10) * 10)

  def run(sparkSession: SparkSession,
          ingestPath: String,
          transformationPath: String): Unit = {
    sparkSession
      .read
      .parquet(ingestPath)
      .withColumn("date", to_date(col("date"), "yyyyMMdd"))
      .withColumn("dayofweek", date_format(col("date"), "EEEE"))
      .withColumn("humidity_range", humidityToRangeUdf(col("hum_avg")))
      .write
      .parquet(transformationPath)
  }
}
