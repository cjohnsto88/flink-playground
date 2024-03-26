#!/usr/bin/env bash

echo -e 'Publishing data'

docker exec flink-kafka /bin/sh -c "echo \"{\"firstName\": \"Craig\", \"lastName\": \"Johnston\"}\" | kafka-console-producer --topic first-flink-data --bootstrap-server kafka:29092"