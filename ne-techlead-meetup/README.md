# TW Northeast Tech Lead Meetup - Hands On Spark

## Installation Instructions

* Install Homebrew (if not installed already)
  * `/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"`
* Install xcode-select
  * `xcode-select --install`
* Install Java: `brew cask install java`
* Install Scala: `brew install scala`
* Install Apache Spark: `brew install apache-spark`
* Install sbt: `brew install sbt@1`
* Download config jar from maven: https://mvnrepository.com/artifact/com.typesafe/config/1.3.2 .
  This is only external dependency needed aparka from Spark packages
* Install Python: `brew install python`

## Kafka Producer Setup Instructions

* clone https://github.com/ThoughtWorksInc/twde-capabilities
* `cd twde-capabilities/kafka-python-producer`
* Install Python packages: `pip3 install --upgrade -r requirements.txt`

## Spark Consumer Instructions

* `cd twde-capabilities/tw-pipeline`
* Build and package: `sbt package`

## How to run (during session)

* Follow instructions from Kafka Producer README.
* Start consumer: spark-submit --jars config-1.3.2.jar --class com.thoughtworks.ca.de.streaming.KafkaConsumer --master local --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 twde-capabilities/tw-pipeline/target/scala-2.11/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar
