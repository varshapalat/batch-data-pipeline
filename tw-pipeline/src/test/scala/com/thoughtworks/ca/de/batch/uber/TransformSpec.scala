package com.thoughtworks.ca.de.batch.uber

import com.thoughtworks.ca.de.DefaultFeatureSpecWithSpark
import org.apache.spark.sql.types.DataTypes

class TransformSpec extends DefaultFeatureSpecWithSpark {
  info("As a user of Transformation object")
  info("I want to be able to apply mapped transformations to data frame")
  info("So it can be used by downstream apps efficiently")

  feature("Apply transformations to data frame") {
    scenario("Uber rides sample data frame") {
      Given("Sample data from uber rides open data")

      import spark.implicits._
      val testDF = Seq(
        ("7/1/2017", "12:00:00 AM", " 874 E 139th St Mott Haven, BX"),
        ("7/1/2017", "12:01:00 AM", " 628 E 141st St Mott Haven, BX"),
        ("7/1/2017", "12:01:00 AM", " 601 E 156th St South Bronx, BX")
      ).toDF("DATE", "TIME", "PICK_UP_ADDRESS")

      When("Transformations are applied")

      val resultDF = Transformation.transform(testDF)

      Then("DATE column is converted to date type and new dayofweek column is added")

      resultDF.schema.fields(0).dataType should be(DataTypes.DateType)
      resultDF.schema.fields(3).name should be("dayofweek")
      resultDF.schema.fields(3).dataType should be(DataTypes.StringType)
    }
  }
}
