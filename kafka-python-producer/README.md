# ThoughtWorks Capabilities Team - Data Engineering Program
## Basic Kafka producer

## Purpose
The purpose of this repo is to generate a stream of messages to demonstrate integration with Spark streaming.
This producer includes sample data from citybikes. It will produce one event every second for almost one hour.

## General setup instructions
* Clone this repo
* Java: `brew cask install java`
* Python: `brew install python`


## Instructions to install Kafka on MacOS
* `brew install kafka`
* `brew services start zookeeper`
* `brew services start kafka`
* `kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic citybikes`
* Optional (if you want to see action): `kafka-console-consumer --bootstrap-server localhost:9092 --topic citybikes --from-beginning`

## Instructions
* Open a new console session and cd to the kafka-python-producer directory
* Run the command: `python kafka_producer.py`
* Go to console where consumer is running to see the magic

## optional arguments
* -f , --file   Sample data file path
* -k , --kafkaServers Comma separated list of Kafka hosts
* -t , --topic Name of topic

## Running this with docker and docker-compose

Assumes docker for Mac version 17.12 onwards

To start: `docker-compose up`  
To stop: `docker-compose down`