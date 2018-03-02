package com.thoughtworks.ca.de.ingest

import org.apache.spark.sql.SparkSession
import com.typesafe.config.{ConfigFactory, ConfigObject, ConfigValue}
import java.util.Map.Entry

import org.apache.log4j.{Level, LogManager, Logger}

import scala.collection.JavaConverters._
import com.thoughtworks.ca.de.utils.{ConfigUtils, CredentialUtils, DateUtils}

object DailyDriver {
  def main(args: Array[String]) {
    val conf = ConfigFactory.load
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val spark =
      SparkSession.builder.appName("Skinny Pipeline: Ingest").getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)

    //Parse argument/s
    var processingDate = DateUtils.date2TWFormat()
    if (!args.isEmpty) {
      processingDate = DateUtils.parseISO2TWFormat(args(0))
    }

    //Set S3 credentials
    CredentialUtils.setCredentialsToContext(spark)

    log.info("Load ingest configurations...")
    val ingestMap = ConfigUtils.getObjectMap(conf,"ngest.sources")
    log.info("Ingest Map: "+ingestMap.toString())

    log.info("Load target configurations...")
    val targetMap = ConfigUtils.getObjectMap(conf,"ingest.output.hdfs.dataSets")
    log.info("Target Map: "+targetMap.toString())

    ingestMap.foreach((ingestConfig)=>{
      val targetPath = conf.getString("ingest.output.hdfs.uri").format(conf.getString("common.hdfs.lake1Path"),
        ingestConfig._1, processingDate)
      log.info("Reading data from: "+ingestConfig._2)
      log.info("writing data to: "+targetPath)
      spark.read.csv(ingestConfig._2).write.parquet(targetPath)
    })

    log.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
  }
}
