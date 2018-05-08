package com.thoughtworks.ca.de.batch.transform

import java.nio.file.Files

import com.thoughtworks.ca.de.DefaultFeatureSpecWithSpark
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

class DailyDriverTest extends DefaultFeatureSpecWithSpark {

  import spark.implicits._

  feature("Transform") {

    scenario("Uber data") {
      val rootDirectory =
        Files.createTempDirectory(this.getClass.getName + "Uber")
      val ingestedDir = rootDirectory.resolve("ingest")
      val transformedDir = rootDirectory.resolve("transform")

      Given("Ingested data")

      Seq(
        ("7/1/2017",
          "12:00:00 AM",
          " 874 E 139th St Mott Haven, BX",
          "",
          "",
          ""),
        ("7/2/2017",
          "12:01:00 AM",
          " 628 E 141st St Mott Haven, BX",
          "",
          "",
          ""),
        ("7/8/2017",
          "12:01:00 AM",
          " 601 E 156th St South Bronx, BX",
          "",
          "",
          ""),
        ("7/10/2017",
          "12:01:00 AM",
          " 708 E 138th St Mott Haven, BX",
          "",
          "",
          "")
      ).toDF("DATE", "TIME", "PICK_UP_ADDRESS", "_4", "_5", "_6")
        .write
        .parquet(ingestedDir.toUri.toString)


      When("Daily Driver Transformation is run for Uber")

      DailyDriver.run(spark,
        ingestedDir.toUri.toString,
        transformedDir.toUri.toString,
        "uberdata")


      Then("The type of the DATE column should be changed to date")

      val transformedDF = spark.read
        .parquet(transformedDir.toUri.toString)
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
        .collect() should be(
        Array("2017-07-01", "2017-07-02", "2017-07-08", "2017-07-10"))


      And("A day of week column should have been added")

      transformedDF
        .map(row => row.getAs[String]("dayofweek"))
        .collect() should be(Array("Saturday", "Sunday", "Saturday", "Monday"))
    }

    scenario("Weather data") {
      val rootDirectory =
        Files.createTempDirectory(this.getClass.getName + "Weather")
      val ingestedDir = rootDirectory.resolve("ingest")
      val transformedDir = rootDirectory.resolve("transform")


      Given("Ingested data")

      val inputDf = Seq(
        ("20170701", 85, 78, 71, 76, 71, 67, 93, 79, 65, 10, 9, 1, 14, 4, 24, 0.23),
        ("20170911", 76, 66, 55, 51, 48, 44, 83, 58, 33, 10, 10, 10, 9, 2, 12, 0D),
        ("20170926", 84, 76, 68, 67, 66, 64, 90, 73, 55, 10, 10, 6, 8, 2, 12, 0D))
        .toDF(
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
          "precip_sum"
        )

      inputDf.write
        .parquet(ingestedDir.toUri.toString)


      When("Daily Driver Transformation is run for Weather")

      DailyDriver.run(spark,
        ingestedDir.toUri.toString,
        transformedDir.toUri.toString,
        "weatherdata")


      Then("The type of the date column should be changed to date")

      val transformedDF = spark.read
        .parquet(transformedDir.toUri.toString)
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
        .collect() should be(
        Array("2017-07-01", "2017-09-11", "2017-09-26"))


      And("A day of week column should have been added")

      transformedDF
        .map(row => row.getAs[String]("dayofweek"))
        .collect() should be(Array("Saturday", "Monday", "Tuesday"))


      And("Humidity should have been bucketed in a new humidity_range column")

      transformedDF
        .map(row => row.getAs[Int]("humidity_range"))
        .collect() should be(Array(80, 60, 70))
    }

    scenario("Citibike data") {
      val rootDirectory =
        Files.createTempDirectory(this.getClass.getName + "Citibike")
      val ingestedDir = rootDirectory.resolve("ingest")
      val transformedDir = rootDirectory.resolve("transform")

      Given("Ingested data")

      val inputDf = Seq(
        (328, "2017-07-01 00:00:08", "2017-07-01 00:05:37", 3242, "Schermerhorn St & Court St", 40.69102925677968, -73.99183362722397, 3397, "Court St & Nelson St", 40.6763947, -73.99869893, 27937, "Subscriber", 1984, 2),
        (1496, "2017-07-01 00:00:18", "2017-07-01 00:25:15", 3233, "E 48 St & 5 Ave", 40.75724567911726, -73.97805914282799, 546, "E 30 St & Park Ave S", 40.74444921, -73.98303529, 15933, "Customer", 1971, 1),
        (1067, "2017-07-01 00:16:31", "2017-07-01 00:34:19", 448, "W 37 St & 10 Ave", 40.75660359, -73.9979009, 487, "E 20 St & FDR Drive", 40.73314259, -73.97573881, 27084, "Subscriber", 1990, 2)
      ).toDF(
        "tripduration", "starttime", "stoptime", "start_station_id", "start_station_name", "start_station_latitude", "start_station_longitude", "end_station_id", "end_station_name", "end_station_latitude", "end_station_longitude", "bikeid", "usertype", "birth_year", "gender"
      )

      inputDf.write
        .parquet(ingestedDir.toUri.toString)


      When("Daily Driver Transformation is run for Bikeshare data")

      DailyDriver.run(spark,
        ingestedDir.toUri.toString,
        transformedDir.toUri.toString,
        "bikesharedata")


      Then("The data should be unchanged")

      val transformedDF = spark.read
        .parquet(transformedDir.toUri.toString)

      transformedDF.collect should be(Array(
        Row(328, "2017-07-01 00:00:08", "2017-07-01 00:05:37", 3242, "Schermerhorn St & Court St", 40.69102925677968, -73.99183362722397, 3397, "Court St & Nelson St", 40.6763947, -73.99869893, 27937, "Subscriber", 1984, 2),
        Row(1496, "2017-07-01 00:00:18", "2017-07-01 00:25:15", 3233, "E 48 St & 5 Ave", 40.75724567911726, -73.97805914282799, 546, "E 30 St & Park Ave S", 40.74444921, -73.98303529, 15933, "Customer", 1971, 1),
        Row(1067, "2017-07-01 00:16:31", "2017-07-01 00:34:19", 448, "W 37 St & 10 Ave", 40.75660359, -73.9979009, 487, "E 20 St & FDR Drive", 40.73314259, -73.97573881, 27084, "Subscriber", 1990, 2)
      ))

      transformedDF.schema.fieldNames should be (inputDf.schema.fieldNames)
      transformedDF.schema.fields.map(_.dataType) should be (inputDf.schema.fields.map(_.dataType))
    }

    scenario("Transit data") {
      val rootDirectory =
        Files.createTempDirectory(this.getClass.getName + "Transit")
      val ingestedDir = rootDirectory.resolve("ingest")
      val transformedDir = rootDirectory.resolve("transform")


      Given("Ingested data")

      val inputDf = Seq(
        ("GS", "A20171105SAT", "A20171105SAT_036000_GS.N01R", "TIMES SQ - 42 ST", "0", "", "GS.N01R"),
        ("1", "A20171105WKD", "A20171105WKD_055800_1..S03R", "SOUTH FERRY", "1", "", "1..S03R"),
        ("5", "A20171105SAT", "A20171105SAT_117200_2..N12R", "WAKEFIELD - 241 ST", "0", "", "")
      ).toDF(
        "route_id", "service_id", "trip_id", "trip_headsign", "direction_id", "block_id", "shape_id"
      )

      inputDf.write
        .parquet(ingestedDir.toUri.toString)


      When("Daily Driver Transformation is run for Transit")

      DailyDriver.run(spark,
        ingestedDir.toUri.toString,
        transformedDir.toUri.toString,
        "transitData")

      Then("The data should be unchanged")

      val transformedDF = spark.read
        .parquet(transformedDir.toUri.toString)

      transformedDF.collect() should be(Array(
        Row("GS", "A20171105SAT", "A20171105SAT_036000_GS.N01R", "TIMES SQ - 42 ST", "0", "", "GS.N01R"),
        Row("1", "A20171105WKD", "A20171105WKD_055800_1..S03R", "SOUTH FERRY", "1", "", "1..S03R"),
        Row("5", "A20171105SAT", "A20171105SAT_117200_2..N12R", "WAKEFIELD - 241 ST", "0", "", "")
      ))

      transformedDF.schema.fieldNames should be (inputDf.schema.fieldNames)
      transformedDF.schema.fields.map(_.dataType) should be (inputDf.schema.fields.map(_.dataType))
    }
  }
}
