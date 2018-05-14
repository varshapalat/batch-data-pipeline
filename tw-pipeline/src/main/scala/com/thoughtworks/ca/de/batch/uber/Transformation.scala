package com.thoughtworks.ca.de.batch.uber

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_format, to_date, udf}

object Transformation {
  def transform(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .withColumn("DATE", to_date(col("DATE"), "MM/dd/yyyy"))
      .withColumn("dayofweek", date_format(col("DATE"), "EEEE"))
  }
}
