# ThoughtWorks Capabilities Team - Data Engineering Program
## Basic Batch Pipeline

[For setup and architecture refer this document] https://docs.google.com/document/d/1F68Vb6qYKba9cqCusJWc7LzuRCUIiCpTrzW-fbb-tTg

## Pipeline structure
* Execute ingest DailyDriver which will ingest and write data to HDFS
* Execute tranform DailyDriver which will transform data from lake 1 and writer to lake 2
* Execute igress DailyDriver which will aggregate and prepare data for data mart in lake 3

## Notes
* Oozie job should be scheduled to run on daily basis (that's why daily drivers)
* New drivers can be created with similar logic to facilitate streaming or more frequant schedules
