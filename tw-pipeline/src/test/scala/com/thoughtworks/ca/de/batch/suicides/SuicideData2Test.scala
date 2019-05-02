package com.thoughtworks.ca.de.batch.suicides

import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.{QueryTest, SparkSession}

class SuicideData2Test extends QueryTest with SharedSQLContext {


  test("calculate total suicides in year by gender") {
    //Write Test
    //Hint: try using checkAnswer()
    val spark: SparkSession = SparkSession.builder
      .appName("Spark Test App")
      .config("spark.driver.host","127.0.0.1")
      .master("local")
      .getOrCreate()
    import spark.implicits._
    val suicideDF = Seq((1990, "M", 20), (1990, "F", 20), (1991, "F", 20), (1989, "F", 20)).toDF("year", "sex", "suicides_no")

    val actualOutput = SuicideData(suicideDF).totalSuicidesInYearByGender(1990)
    val expectedOutput = Seq(
      ("F", 20),
      ("M", 20)
    ).toDF( "sex", "sum(suicides_no)")

    actualOutput.show()
    expectedOutput.show()

    checkAnswer(actualOutput, expectedOutput)
  }
}
