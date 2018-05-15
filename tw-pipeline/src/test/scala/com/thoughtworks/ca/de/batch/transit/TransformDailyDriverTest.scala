package com.thoughtworks.ca.de.batch.transit

import java.nio.file.Files

import com.thoughtworks.ca.de.DefaultFeatureSpecWithSpark
import org.apache.spark.sql.Row

class TransformDailyDriverTest extends DefaultFeatureSpecWithSpark {

  import spark.implicits._

  val transitDataColumns = Seq("route_id", "service_id", "trip_id", "trip_headsign", "direction_id", "block_id", "shape_id")
  val sampleTransitData = Seq(
    ("GS", "A20171105SAT", "A20171105SAT_036000_GS.N01R", "TIMES SQ - 42 ST", "0", "", "GS.N01R"),
    ("1", "A20171105WKD", "A20171105WKD_055800_1..S03R", "SOUTH FERRY", "1", "", "1..S03R"),
    ("5", "A20171105SAT", "A20171105SAT_117200_2..N12R", "WAKEFIELD - 241 ST", "0", "", "")
  )

  feature("Transform") {
    scenario("Transit data") {

      val (ingestDir, transformDir) = makeInputAndOutputDirectories("Transit")

      Given("Ingested data")

      val inputDF = sampleTransitData.toDF(transitDataColumns: _*)

      inputDF.write
        .parquet(ingestDir)


      When("Daily Driver Transformation is run for Transit")

      TransformDailyDriver.run(spark, ingestDir, transformDir)

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
