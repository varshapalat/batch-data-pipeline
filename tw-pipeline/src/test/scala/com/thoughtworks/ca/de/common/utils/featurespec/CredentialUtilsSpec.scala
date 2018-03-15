package com.thoughtworks.ca.de.common.utils.featurespec

import org.scalatest._
import com.thoughtworks.ca.de.common.utils.CredentialUtils
import org.apache.spark.sql.SparkSession

class CredentialUtilsSpec extends FeatureSpec with GivenWhenThen{
  info("As a user of Credential Utilities")
  info("I want to be able to set third party credentials using common cofigurations to Spark Session")
  info("So it can be used to read and/or write data to/from third party systems")

  feature("Set common credentials") {
    scenario("User invokes method to set common credentials") {

      Given("Spark session")
      val spark =
        SparkSession.builder.appName("Credential Test App").master("local").getOrCreate()

      When("Credential utils is used")
      CredentialUtils.setCredentialsToContext(spark)

      Then("Credentials are set as defined in application.conf")
      assert(spark.sparkContext.hadoopConfiguration.get("fs.s3n.awsAccessKeyId").equals("testKeyId"))
      assert(spark.sparkContext.hadoopConfiguration.get("fs.s3n.awsSecretAccessKey").equals("testKey"))
      assert(spark.conf.get("spark.cassandra.connection.host").equals("testHost"))
      assert(spark.conf.get("spark.cassandra.auth.username").equals("testUser"))
      assert(spark.conf.get("spark.cassandra.auth.password").equals("testPwd"))
    }
  }
}
