package com.thoughtworks.ca.de.batch.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{date_format,col,to_date,udf}

object Transformation {
  //Sample udf
  def humidityToRangeUdf=udf((humidity:Int)=>((humidity+5)/10)*10)

  //Dataframe transformation functions
  def uberTransformation (dataFrame: DataFrame)={
    dataFrame.withColumn("DATE",to_date(col("DATE"),"MM/dd/yyyy")).
      withColumn("dayofweek",date_format(col("DATE"),"EEEE"))
  }
  def weatherTransformation (dataFrame: DataFrame)={
    dataFrame.withColumn("date",to_date(col("date"),"yyyyMMdd")).
      withColumn("dayofweek",date_format(col("date"),"EEEE"))
        .withColumn("humidity_range",humidityToRangeUdf(col("hum_avg")))
  }
  def transitTransformation (dataFrame: DataFrame)=dataFrame
  def bikeshareTransformation (dataFrame: DataFrame)=dataFrame

  //Mapping of transformation functions to date set ids
  val transformationMap=Map[String, (DataFrame) => DataFrame](
    "uberdata"->uberTransformation
    ,"weatherdata"->weatherTransformation
    ,"transitData"->transitTransformation
    ,"bikesharedata"->bikeshareTransformation
  )

  def transform(dataFrame: DataFrame,datasetId:String):DataFrame={
    dataFrame.printSchema()
    transformationMap(datasetId)(dataFrame)
  }
}
