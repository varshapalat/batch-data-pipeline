package com.thoughtworks.ca.de.batch.suicides

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSQLContext

class SuicideData2Test extends QueryTest with SharedSQLContext {

  import testImplicits._

  test("calculate total suicides in year by gender") {
    //Write Test
    //Hint: try using checkAnswer()
  }
}
