# ThoughtWorks Capabilities Team - Data Engineering Program
## Basic Batch Pipeline

## Pipeline structure
* Execute ingest_to_data_lake DailyDriver which will ingest data and write as parquet to HDFS
* Data applications like citibike should use data from HDFS in parquet and write to a data mart 

## Pre-requisites
* Java
* Scala
* Sbt
* Apache Spark
* Hadoop (optional)

## Setup Process
* Clone the repo
* `cd twde-capabilities/tw-pipeline`
* Build: sbt package
* Test: sbt test
* Note: Update src/test/resources/application.conf to change configurations for testing

## Running pipeline
* Sample data: https://drive.google.com/drive/folders/120TuvQCLjvYdpJb_I4Hw83WT7rquFjs5
* Note: Please change path to data files in application.conf before running pipeline. The file is located in src/main/resources
* Ingest data to datalake: `spark-submit --jars config-1.3.2.jar --class com.thoughtworks.ca.de.batch.ingest.DailyDriver --master local target/scala-2.11/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar`
* Transform data : `spark-submit --jars config-1.3.2.jar --class com.thoughtworks.ca.de.batch.transform.DailyDriver --master local target/scala-2.11/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar`
* Sample App: `spark-submit --jars config-1.3.2.jar --class com.thoughtworks.ca.de.batch.app.UberRidesByHumidityRange --master local twde-capabilities/tw-pipeline/target/scala-2.11/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar`

## Instructions to install airflow (local)
* Install Python 2.7 as airflow is on compatible with Python 3 yet: `pip install python@2`
* Install virtual environment: `pip install virtualenv`
* Create vm for airflow: `virtualenv airflow_vm`
* `cd airflow_vm/`
* `source bin/activate`
* `pip install airflow`
* `airflow initdb`
* `airflow webserver -p 9095`