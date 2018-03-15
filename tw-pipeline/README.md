# ThoughtWorks Capabilities Team - Data Engineering Program
## Basic Batch Pipeline

[For setup and architecture refer this document] https://docs.google.com/document/d/1F68Vb6qYKba9cqCusJWc7LzuRCUIiCpTrzW-fbb-tTg

## Pipeline structure
* Execute ingest DailyDriver which will ingest and write data to HDFS
* Execute tranform DailyDriver which will transform data from lake 1 and writer to lake 2
* Data applications should use data from lake 2 and write to lake 3 (for audit purpose) and to target data marts (like Cassandra)

## Pre-reqs
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