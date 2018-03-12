package com.thoughtworks.ca.de.streaming

import com.typesafe.config.ConfigFactory
import org.apache.log4j.LogManager
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, TimestampType}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    val log = LogManager.getRootLogger
    val spark = SparkSession.builder
      .appName("StructuredNetworkWordCount")
      .getOrCreate()
    import spark.implicits._

    /*
    Writer code here to:
    1. read citybikes topic stream from kafka
    2. Convert kafka JSON payload to dataframe rows
    3. Convert start time and stop time columns to Timestamp
    4. Convert birth year column to Date
    5. Count number of rides per minute window
    6. Write to file system/hdfs in parquet format partitioned by minute window
     */
    val schema = DataTypes.createStructType(
      Array[StructField](
        DataTypes
          .createStructField("tripduration", DataTypes.StringType, false),
        DataTypes.createStructField("starttime", DataTypes.StringType, false),
        DataTypes.createStructField("stoptime", DataTypes.StringType, false),
        DataTypes
          .createStructField("start station id", DataTypes.StringType, false),
        DataTypes
          .createStructField("start station name", DataTypes.StringType, false),
        DataTypes.createStructField("start station latitude",
          DataTypes.StringType,
          false),
        DataTypes
          .createStructField("end station id", DataTypes.StringType, false),
        DataTypes
          .createStructField("end station name", DataTypes.StringType, false),
        DataTypes.createStructField("end station latitude",
          DataTypes.StringType,
          false),
        DataTypes.createStructField("end station longitude",
          DataTypes.StringType,
          false),
        DataTypes.createStructField("bikeid", DataTypes.StringType, false),
        DataTypes.createStructField("usertype", DataTypes.StringType, false),
        DataTypes.createStructField("birth year", DataTypes.StringType, false),
        DataTypes.createStructField("gender", DataTypes.StringType, false)
      ))

    val bikesDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",
        conf.getString("streaming.kafka.hosts"))
      .option("subscribe", conf.getString("streaming.kafka.topic"))
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING) as message")
      .select(functions.from_json(functions.col("message"), schema).as("json"))
      .select("json.*")
      .withColumn("starttime", ($"starttime".cast(TimestampType)))
      .withColumn("stoptime", ($"stoptime".cast(TimestampType)))
      .withColumn("birth year", ($"birth year".cast(DateType)))

    bikesDF.printSchema()

    val countPerMinute = bikesDF
      .withWatermark("starttime", "1 minutes")
      .groupBy(
        window($"starttime", "1 minutes"),
        $"bikeid"
      )
      .count()

    countPerMinute.printSchema()

    countPerMinute.writeStream
      .format("parquet")
      .option("path", conf.getString("streaming.output"))
      .option("checkpointLocation", conf.getString("streaming.checkpoint"))
      .partitionBy("window")
      .trigger(Trigger.ProcessingTime("1 minutes"))
      .start()
      .awaitTermination()

  }
}
