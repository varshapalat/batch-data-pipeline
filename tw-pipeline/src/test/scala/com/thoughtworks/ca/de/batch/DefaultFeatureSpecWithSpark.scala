package com.thoughtworks.ca.de.batch

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FeatureSpec, GivenWhenThen, Matchers}

class DefaultFeatureSpecWithSpark extends FeatureSpec with GivenWhenThen with Matchers {
  val spark: SparkSession = SparkSession.builder.appName("Spark Test App").master("local").getOrCreate()
}
