#!/usr/bin/env bash

echo -e 'Creating kafka topics'
docker exec flink-kafka kafka-topics --create --if-not-exists --topic first-flink-data --partitions 2 --replication-factor 1 --bootstrap-server kafka:29092