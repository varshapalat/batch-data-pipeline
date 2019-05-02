package com.thoughtworks.ca.de.batch.suicides

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object SuicideUtils {
  implicit class SuicideDataset(val dataSet: Dataset[Row]) {

    def totalSuicidesInYearByGender(spark: SparkSession, inYear: Integer): Dataset[Row] = {
      dataSet
    }
  }
}
