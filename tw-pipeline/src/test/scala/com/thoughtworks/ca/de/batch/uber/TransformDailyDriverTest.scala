package com.thoughtworks.ca.de.batch.uber

import java.nio.file.Files

import com.thoughtworks.ca.de.DefaultFeatureSpecWithSpark
import org.apache.commons.lang3.time.DateFormatUtils
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

  feature("Transform") {
    scenario("Uber data") {
      val (ingestDir, transformDir) = makeInputAndOutputDirectories("Uber")

      Given("Ingested data")

      sampleUberData.toDF(uberDataColumns: _*)
        .write
        .parquet(ingestDir)


      When("Daily Driver Transformation is run for Uber")

      TransformDailyDriver.run(spark, ingestDir, transformDir)


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
  }

  private def makeInputAndOutputDirectories(folderNameSuffix: String): (String, String) = {
    val rootDirectory =
      Files.createTempDirectory(this.getClass.getName + folderNameSuffix)
    val ingestDir = rootDirectory.resolve("ingest")
    val transformDir = rootDirectory.resolve("transform")
    (ingestDir.toUri.toString, transformDir.toUri.toString)
  }
}
