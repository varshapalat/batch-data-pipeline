package com.thoughtworks.ca.de

import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import collection.JavaConversions._
import org.apache.log4j.{Level, LogManager, PropertyConfigurator}

class SkinnyPipelineDriver {
  def main(args: Array[String]) {
    val conf = ConfigFactory.load
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val spark = SparkSession.builder.appName("Skinny Pipeline").getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)

    //Set S3 credentials
    log.info("Intializing S3 Credentials...")
    spark.sparkContext.hadoopConfiguration
      .set("fs.s3n.awsAccessKeyId", conf.getString("input.s3.awsAccessKeyId"))
    spark.sparkContext.hadoopConfiguration.set(
      "fs.s3n.awsSecretAccessKey",
      conf.getString("input.s3.awsSecretAccessKey"))
    log.info("Intializing S3 Credentials done")

    //Set Cassandra configurations
    log.info("Intializing Cassandra Credentials...")
    spark.conf.set("spark.cassandra.connection.host",
                   conf.getString("output.cassandra.host"))
    spark.conf.set("spark.cassandra.auth.username",
                   conf.getString("output.cassandra.username"))
    spark.conf.set("spark.cassandra.auth.password",
                   conf.getString("output.cassandra.password"))
    log.info("Intializing Cassandra Credentials done")

    //Read flight data from S3
    log.info("Reading flight data...")
    val flightData = spark.read.parquet(
      conf
        .getString("input.s3.url")
        .format(conf.getString("input.s3.region"),
                conf.getString("input.s3.bucket"),
                conf.getString("input.s3.file")))
    log.info("Reading flight data done")

    log.info("Describe flight data")
    flightData.printSchema()

    //Save flight data to Cassandra
    log.info("Writing data to Cassandra...")
    flightData.rdd.saveToCassandra(
      conf.getString("output.cassandra.keySpaceName"),
      conf.getString("output.cassandra.tableName"),
      SomeColumns("key", "value"))
    log.info("Writing data to Cassandra done")

    log.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
  }
}
