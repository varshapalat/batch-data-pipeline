package com.thoughtworks.ca.de.batch.wordcount

import org.apache.spark.sql.{Dataset, SparkSession}

object WordCountUtils {
  implicit class StringDataset(val dataSet: Dataset[String]) {
    def splitWords(spark: SparkSession) = {
      import spark.implicits._
      dataSet.flatMap(line => line.split(" "))
    }

    def countByWord(spark: SparkSession) = {
      import spark.implicits._
      val df = dataSet.map(word => word.toLowerCase)
        .map(word => word.replaceAll(",", ""))
        .map(word => (word, 1))
        .groupBy("_1")
        .count()
      df.as[(String, Long)]
//      spark.emptyDataset[(String, Long)]
    }
  }

  implicit class StringLongDataset(val dataSet: Dataset[(String, Long)]) {
    def sortByWord(spark: SparkSession) = {
      dataSet.sort("_1")
    }
  }
}
