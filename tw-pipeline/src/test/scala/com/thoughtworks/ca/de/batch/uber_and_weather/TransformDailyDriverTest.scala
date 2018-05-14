package com.thoughtworks.ca.de.batch.uber_and_weather

import java.nio.file.Files

import com.thoughtworks.ca.de.DefaultFeatureSpecWithSpark
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

class TransformDailyDriverTest extends DefaultFeatureSpecWithSpark {

  import spark.implicits._

  val uberDataColumns = Seq("DATE", "TIME", "PICK_UP_ADDRESS", "_4", "_5", "_6")
  val sampleUberData = Seq(
    ("7/1/2017", "12:00:00 AM", " 874 E 139th St Mott Haven, BX", "", "", ""),
    ("7/2/2017", "12:01:00 AM", " 628 E 141st St Mott Haven, BX", "", "", ""),
    ("7/8/2017", "12:01:00 AM", " 601 E 156th St South Bronx, BX", "", "", ""),
    ("7/10/2017", "12:01:00 AM", " 708 E 138th St Mott Haven, BX", "", "", "")
  )

  val weatherDataColumns = Seq(
    "date",
    "temp_high",
    "temp_avg",
    "temp_low",
    "dp_high",
    "dp_avg",
    "dp_low",
    "hum_high",
    "hum_avg",
    "hum_low",
    "vis_high",
    "vis_avg",
    "vis_low",
    "wind_low",
    "wind_avg",
    "wind_high",
    "precip_sum")
  val sampleWeatherData = Seq(
    ("20170701", 85, 78, 71, 76, 71, 67, 93, 79, 65, 10, 9, 1, 14, 4, 24, 0.23),
    ("20170911", 76, 66, 55, 51, 48, 44, 83, 58, 33, 10, 10, 10, 9, 2, 12, 0D),
    ("20170926", 84, 76, 68, 67, 66, 64, 90, 73, 55, 10, 10, 6, 8, 2, 12, 0D)
  )

  val transitDataColumns = Seq("route_id", "service_id", "trip_id", "trip_headsign", "direction_id", "block_id", "shape_id")
  val sampleTransitData = Seq(
    ("GS", "A20171105SAT", "A20171105SAT_036000_GS.N01R", "TIMES SQ - 42 ST", "0", "", "GS.N01R"),
    ("1", "A20171105WKD", "A20171105WKD_055800_1..S03R", "SOUTH FERRY", "1", "", "1..S03R"),
    ("5", "A20171105SAT", "A20171105SAT_117200_2..N12R", "WAKEFIELD - 241 ST", "0", "", "")
  )

  feature("Transform") {

    scenario("Uber data") {
      val (ingestDir, transformDir) = makeInputAndOutputDirectories("Uber")

      Given("Ingested data")

      sampleUberData.toDF(uberDataColumns: _*)
        .write
        .parquet(ingestDir)


      When("Daily Driver Transformation is run for Uber")

      TransformDailyDriver.run(spark, ingestDir, transformDir, "uberdata")


      Then("The type of the DATE column should be changed to date")

      val transformedDF = spark.read
        .parquet(transformDir)
      transformedDF
        .schema("DATE")
        .dataType should be(DataTypes.DateType)


      And("Dates should have been parsed correctly")

      transformedDF
        .withColumn(
          "DATE",
          date_format(col("DATE"),
            DateFormatUtils.ISO_8601_EXTENDED_DATE_FORMAT.getPattern))
        .map(row => row.getAs[String]("DATE"))
        .collect should be(Array("2017-07-01", "2017-07-02", "2017-07-08", "2017-07-10"))


      And("A day of week column should have been added")

      transformedDF
        .map(row => row.getAs[String]("dayofweek"))
        .collect should be(Array("Saturday", "Sunday", "Saturday", "Monday"))
    }

    scenario("Weather data") {
      val (ingestDir, transformDir) = makeInputAndOutputDirectories("Weather")

      Given("Ingested data")


      sampleWeatherData.toDF(weatherDataColumns: _*)
        .write
        .parquet(ingestDir)


      When("Daily Driver Transformation is run for Weather")

      TransformDailyDriver.run(spark, ingestDir, transformDir, "weatherdata")


      Then("The type of the date column should be changed to date")

      val transformedDF = spark.read
        .parquet(transformDir)
      transformedDF
        .schema("date")
        .dataType should be(DataTypes.DateType)


      And("Dates should have been parsed correctly")

      transformedDF
        .withColumn(
          "date",
          date_format(col("date"),
            DateFormatUtils.ISO_8601_EXTENDED_DATE_FORMAT.getPattern))
        .map(row => row.getAs[String]("date"))
        .collect should be(Array("2017-07-01", "2017-09-11", "2017-09-26"))


      And("A day of week column should have been added")

      transformedDF
        .map(row => row.getAs[String]("dayofweek"))
        .collect should be(Array("Saturday", "Monday", "Tuesday"))


      And("Humidity should have been bucketed in a new humidity_range column")

      transformedDF
        .map(row => row.getAs[Int]("humidity_range"))
        .collect should be(Array(80, 60, 70))
    }

    scenario("Transit data") {

      val (ingestDir, transformDir) = makeInputAndOutputDirectories("Transit")

      Given("Ingested data")

      val inputDF = sampleTransitData.toDF(transitDataColumns: _*)

      inputDF.write
        .parquet(ingestDir)


      When("Daily Driver Transformation is run for Transit")

      TransformDailyDriver.run(spark, ingestDir, transformDir, "transitData")

      Then("The data should be unchanged")

      val transformedDF = spark.read
        .parquet(transformDir)

      transformedDF.collect should be(sampleTransitData.map(t => Row(t.productIterator.toList: _*)).toArray)

      transformedDF.schema.fieldNames should be(inputDF.schema.fieldNames)
      transformedDF.schema.fields.map(_.dataType) should be(inputDF.schema.fields.map(_.dataType))
    }
  }

  private def makeInputAndOutputDirectories(folderNameSuffix: String): (String, String) = {
    val rootDirectory =
      Files.createTempDirectory(this.getClass.getName + folderNameSuffix)
    val ingestDir = rootDirectory.resolve("ingest")
    val transformDir = rootDirectory.resolve("transform")
    (ingestDir.toUri.toString, transformDir.toUri.toString)
  }
}
