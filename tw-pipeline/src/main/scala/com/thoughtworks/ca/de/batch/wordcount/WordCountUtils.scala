package com.thoughtworks.ca.de.batch.wordcount

import org.apache.spark.sql.{Dataset, SparkSession}

object WordCountUtils {
  implicit class StringDataset(val dataSet: Dataset[String]) {
    def splitWords(spark: SparkSession) = {
      dataSet
    }

    def countByWord(spark: SparkSession) = {
      import spark.implicits._
      spark.emptyDataset[(String, Long)]
    }
  }

  implicit class StringLongDataset(val dataSet: Dataset[(String, Long)]) {
    def sortByWord(spark: SparkSession) = {
      dataSet
    }
  }
}
