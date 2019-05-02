package com.thoughtworks.ca.de.batch.suicides

import com.thoughtworks.ca.de.DefaultFeatureSpecWithSpark
import org.scalatest.Matchers

class SuicideDataTest extends DefaultFeatureSpecWithSpark with Matchers {

  feature("groupby gender and calculate sum of suicides for a year") {
    scenario("calculate total suicides in year by gender") {
      //Write Test
      //Hint: try using except()

      import spark.implicits._

      val suicideDF = Seq((1990, "M", 20), (1990, "F", 20), (1991, "F", 20), (1989, "F", 20)).toDF("year", "sex", "suicides_no")


      val actualOutput = SuicideData(suicideDF).totalSuicidesInYearByGender(1990)
      val expectedOutput = Seq(
        ("F", 20),
        ("M", 20)
      ).toDF( "sex", "sum(suicides_no)")

      actualOutput.show()
      expectedOutput.show()
      val extraElements1 = actualOutput.except(expectedOutput)
      val extraElements2 = expectedOutput.except(actualOutput)
      assert(extraElements1.count()==0)
      assert(extraElements2.count()==0)
    }
  }
}
