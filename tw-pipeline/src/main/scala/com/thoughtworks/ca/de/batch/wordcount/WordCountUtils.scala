package com.thoughtworks.ca.de.batch.wordcount

import org.apache.spark.sql.{Dataset, SparkSession}

object WordCountUtils {
  implicit class StringDataset(val dataSet: Dataset[String]) {
    def splitWords(spark: SparkSession) = {
      dataSet
    }

    def countByWord(spark: SparkSession) = {
      dataSet
    }

    def sortByWord(spark: SparkSession) = {
      dataSet
    }
  }
}
