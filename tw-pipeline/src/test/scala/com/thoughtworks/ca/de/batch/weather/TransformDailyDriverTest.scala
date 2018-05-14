package com.thoughtworks.ca.de.batch.weather

import java.nio.file.Files

import com.thoughtworks.ca.de.DefaultFeatureSpecWithSpark
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

class TransformDailyDriverTest extends DefaultFeatureSpecWithSpark {

  import spark.implicits._

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


  feature("Transform") {

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

  }

  private def makeInputAndOutputDirectories(folderNameSuffix: String): (String, String) = {
    val rootDirectory =
      Files.createTempDirectory(this.getClass.getName + folderNameSuffix)
    val ingestDir = rootDirectory.resolve("ingest")
    val transformDir = rootDirectory.resolve("transform")
    (ingestDir.toUri.toString, transformDir.toUri.toString)
  }
}
