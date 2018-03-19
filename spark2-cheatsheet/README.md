# Spark 2 Cheatsheet

## Spark Session
* Import: `import org.apache.spark.sql.SparkSession`
* Create new spark session: `val spark = SparkSession.builder.appName("AppName").getOrCreate()`

## Ways to create RDDs
* From a list (array in memory): `spark.parallelize(Seq(("maths",52),("english",75),("science",82), ("computer",65),("maths",85)))`
** This will create an RDD of tupples
* From a persistent layer (like hdfs): `spark.sparkContext.textFile("filePath")`
* Binary files: `spark.sparkContext.binaryFiles("filePath")`
* From dataframe: df.rdd

## Ways to create DataFrames
* From a list
** `import spark.implicits._`
** `val df = Seq(("maths",52),("english",75),("science",82), ("computer",65),("maths",85)).toDF("subject","percent")`
* From a list with detailed schema
  * `val array=Seq(("maths",52),("english",75),("science",82), ("computer",65),("maths",85))`
  * `val schema = List(StructField("subject", StringType, true),StructField("percent", IntegerType, true))`
  * `val df = spark.createDataFrame(spark.sparkContext.parallelize(array),StructType(schema))`
* From a list using createDF method
  * `val df=spark.createDF(Seq(("maths",52),("english",75),("science",82), ("computer",65),("maths",85)),
      List(("subject", StringType, true),("percent", IntegerType, true)))`
* From a persistent layer (like hdfs)
  * Without schema: `spark.read.csv("filePath")`
  * With schema: `spark.read.csv("filePath").schema(List(("subject", StringType, true),("percent", IntegerType, true)))`
  * Formats Supported:
    * parquet
    * csv
    * jdbc
    * json
    * orc
    * table (hive table)
    * text
    * textFile (for datasets)
* From rdd: rdd.toDF

## How to create datasets
* `case class Person(name: String, age: Long)`
* `val personDS = Seq(Person("Andy", 32)).toDS()`

## DataFrame Operations
* Print schema to console: `df.printSchema()`
* Select subset of columns (to create new df): `val subDF = df.select("name","age")`
* Filter records: `val subDF = df.filter($"age"<21)`
* Group by: `df.groupBy("column1").count()`
* Explore Data
  * `df.take(10)`
  * `df.head(5)`
  * `df.count()`
  * `df.columns()`
  * `df.describe()`
  * `df.printSchema()`
  * `df.sample()`
* SQL (not Spark SQL yet)
  * Select subset of columns: `df.select("column1","column2")`
  * Remove duplicates: `df.distinct()` or `df.dropDuplicates()`
  * Drop all null rows: `df.dropna()`
  * Replace null values: `df.fillna("Not Applicable")`
  * Aggregations: `df.groupBy("column1").agg({'measure': 'mean'})`
  * Map: `df.map(lambda row:(row.getString(0).toLowerCase()))`
  * Sort: `df.orderBy($"column1")` or `df.orderBy(desc($"column1"))`
  * Add new column: `df.withColumn("newColumn",$"column1"-$"column2")`
  * Drop column: `df.drop("column1")`
* Cross DataFrame Operations
  * Join: `df1.join(df2,df1.col("main_id")===df2.col("reference_id"))`
  * Left outer join: `df1.join(df2,df1.col("main_id")===df2.col("reference_id"),"left_outer")`
  * Substract: `df1.substract(df2)` or `df1.except(df2)`
  * Union: `df1.union(df2)`
  * Decrease number of partitions: `df.coalesce(n)`
  * Change number of partitions (use only when increasing): `df.repartition(n)`

## Spark SQL
* Register dataframe as table: `df.registerAsTable("customer_billing")`
* Example SQL creates new dataframe: `sqlContext.sql("select customer_id,billing_amt from customer_billing")`
* List of sql functions: https://spark.apache.org/docs/2.0.2/api/java/org/apache/spark/sql/functions.html
* Available SQL literals (Follows HiveQL convention):
  * select
  * from
  * where
  * count
  * having
  * group by
  * order by
  * sort by
  * distinct
  * join
  * (left|right|full) outer join
