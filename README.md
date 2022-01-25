# Overview
This project built a streaming data pipeline of Twitter tweets, transform them and store them into ElasticSearch.

# Steps
* Developed Kafka Producer: Built a Kafka Producer that connect "kafka" related tweets from Twitter API to a kafka Topic with hbc Java HTTP client
* Transform data with Kafka Streams API within Kafka topics: Filtered for tweets from users with 100k followers only
* Implemented Kafka Consumer: The Kafka Consumer polled tweets from Kafka Topic and then sank data into Elastic Search

