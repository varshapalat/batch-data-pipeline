package com.thoughtworks.ca.de.common.utils

import com.typesafe.config.ConfigFactory
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object CredentialUtils {
  def setCredentialsToContext(spark: SparkSession): Unit ={
    val conf = ConfigFactory.load
    val log = LogManager.getRootLogger
    //Set S3 credentials
    log.info("Intializing S3 Credentials...")
//    spark.sparkContext.hadoopConfiguration
    //      .set("fs.s3n.awsAccessKeyId", conf.getString("common.credentials.s3.awsAccessKeyId"))
    //    spark.sparkContext.hadoopConfiguration.set(
    //      "fs.s3n.awsSecretAccessKey",
    //      conf.getString("common.credentials.s3.awsSecretAccessKey"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint"
      , "s3.us-west-2.amazonaws.com.com")
    spark.sparkContext.hadoopConfiguration.
      set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
    log.info("Intializing S3 Credentials done")

    //Set Cassandra configurations
    log.info("Intializing Cassandra Credentials...")
    spark.conf.set("spark.cassandra.connection.host",
      conf.getString("common.credentials.cassandra.host"))
    spark.conf.set("spark.cassandra.auth.username",
      conf.getString("common.credentials.cassandra.username"))
    spark.conf.set("spark.cassandra.auth.password",
      conf.getString("common.credentials.cassandra.password"))
    log.info("Intializing Cassandra Credentials done")
  }
}
