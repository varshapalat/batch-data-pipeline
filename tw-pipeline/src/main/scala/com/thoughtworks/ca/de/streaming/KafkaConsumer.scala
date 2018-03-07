package com.thoughtworks.ca.de.streaming

import com.typesafe.config.ConfigFactory
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    val log = LogManager.getRootLogger
    val spark = SparkSession.builder
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    /*
    Writer code here to:
    1. read citybikes topic stream from kafka
    2. Convert kafka JSON payload to dataframe rows
    3. Convert start time and stop time columns to Timestamp
    4. Convert birth year column to Date
    5. Count number of rides per minute window
    6. Write to file system/hdfs in parquet format partitioned by minute window
     */

  }
}
