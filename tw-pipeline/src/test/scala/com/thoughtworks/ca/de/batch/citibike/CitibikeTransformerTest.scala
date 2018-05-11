package com.thoughtworks.ca.de.batch.citibike

import java.nio.file.Files

import com.thoughtworks.ca.de.DefaultFeatureSpecWithSpark
import org.apache.spark.sql.{Row}
import org.apache.spark.sql.types.{DoubleType, StructField}

class CitibikeTransformerTest extends DefaultFeatureSpecWithSpark {

  import spark.implicits._

  val citibikeDataColumns = Seq(
    "tripduration", "starttime", "stoptime", "start_station_id", "start_station_name", "start_station_latitude", "start_station_longitude", "end_station_id", "end_station_name", "end_station_latitude", "end_station_longitude", "bikeid", "usertype", "birth_year", "gender"
  )
  val sampleCitibikeData = Seq(
    (328, "2017-07-01 00:00:08", "2017-07-01 00:05:37", 3242, "Schermerhorn St & Court St", 40.69102925677968, -73.99183362722397, 3397, "Court St & Nelson St", 40.6763947, -73.99869893, 27937, "Subscriber", 1984, 2),
    (1496, "2017-07-01 00:00:18", "2017-07-01 00:25:15", 3233, "E 48 St & 5 Ave", 40.75724567911726, -73.97805914282799, 546, "E 30 St & Park Ave S", 40.74444921, -73.98303529, 15933, "Customer", 1971, 1),
    (1067, "2017-07-01 00:16:31", "2017-07-01 00:34:19", 448, "W 37 St & 10 Ave", 40.75660359, -73.9979009, 487, "E 20 St & FDR Drive", 40.73314259, -73.97573881, 27084, "Subscriber", 1990, 2)
  )

  feature("Citibike Application") {
    scenario("Citibike Acceptance Test") {
      val rootDirectory = Files.createTempDirectory(this.getClass.getName + "Citibike")
      val ingestedDir = rootDirectory.resolve("ingest")
      val transformedDir = rootDirectory.resolve("transform")

      Given("Ingested data")

      val inputDf = sampleCitibikeData.toDF(citibikeDataColumns: _*)

      inputDf.write
        .parquet(ingestedDir.toUri.toString)


      When("Daily Driver Transformation is run for Bikeshare data")

      CitibikeTransformer.run(spark, ingestedDir.toUri.toString, transformedDir.toUri.toString)


      Then("The data should contain a distance column")

      val transformedDF = spark.read
        .parquet(transformedDir.toUri.toString)

      val expectedData = Array(
        Row(328, "2017-07-01 00:00:08", "2017-07-01 00:05:37", 3242, "Schermerhorn St & Court St", 40.69102925677968, -73.99183362722397, 3397, "Court St & Nelson St", 40.6763947, -73.99869893, 27937, "Subscriber", 1984, 2, 1.01),
        Row(1496, "2017-07-01 00:00:18", "2017-07-01 00:25:15", 3233, "E 48 St & 5 Ave", 40.75724567911726, -73.97805914282799, 546, "E 30 St & Park Ave S", 40.74444921, -73.98303529, 15933, "Customer", 1971, 1, 0.88),
        Row(1067, "2017-07-01 00:16:31", "2017-07-01 00:34:19", 448, "W 37 St & 10 Ave", 40.75660359, -73.9979009, 487, "E 20 St & FDR Drive", 40.73314259, -73.97573881, 27084, "Subscriber", 1990, 2.0, 1.62)
      )

      transformedDF.schema("distance") should be (StructField("distance", DoubleType, nullable = true))
      transformedDF.collect should be(expectedData)

    }


  }
}