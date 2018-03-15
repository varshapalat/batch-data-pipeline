package com.thoughtworks.ca.de.batch.transform.featurespec

import org.scalatest._
import com.thoughtworks.ca.de.batch.transform.Transformation
import org.apache.spark.sql.SparkSession

class TransformSpec extends FeatureSpec with GivenWhenThen{
  info("As a user of Transformation object")
  info("I want to be able to apply mapped transformations to data frame")
  info("So it can be used by downstream apps efficiently")

  feature("Apply transformations to data frame") {
    scenario("Uber rides sample data frame") {

      Given("Sample data from uber rides open data")
      val spark =
        SparkSession.builder.appName("Credential Test App").master("local").getOrCreate()
      import spark.implicits._
      val testDF = Seq(
        ("7/1/2017","12:00:00 AM"," 874 E 139th St Mott Haven, BX"),
        ("7/1/2017","12:01:00 AM"," 628 E 141st St Mott Haven, BX"),
        ("7/1/2017","12:01:00 AM"," 601 E 156th St South Bronx, BX")
      ).toDF("DATE","TIME","PICK_UP_ADDRESS")

      When("Transformations are applied")
      val resultDF = Transformation.transform(testDF,"uberdata")

      Then("DATE column is converted to date type and new dayofweek column is added")
      assert(resultDF.schema.fields(0).dataType.typeName.equals("date"))
      assert(resultDF.schema.fields(3).name.equals("dayofweek"))
      assert(resultDF.schema.fields(3).dataType.typeName.equals("string"))
    }
  }
}
