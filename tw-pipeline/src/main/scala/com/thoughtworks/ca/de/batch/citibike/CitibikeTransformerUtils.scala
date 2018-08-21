package com.thoughtworks.ca.de.batch.citibike

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object CitibikeTransformerUtils {
  implicit class StringDataset(val dataSet: Dataset[Row]) {
    private final val MetersPerFoot = 0.3048
    private final val FeetPerMile = 5280

    final val EarthRadiusInM: Double = 6371e3
    final val MetersPerMile: Double = MetersPerFoot * FeetPerMile

    def computeDistances(spark: SparkSession) = {
      dataSet
    }
  }
}
