# Flink Playground

## Handy Commands

### Start the Kafka Container
```shell
docker-compose up -d
```

### Enter Kafka Container
```shell
docker exec -it flink-kafka bash
```

### Publish to Kafka Topic 
From inside the Kafka container
```shell
echo "{\"firstName\": \"John\", \"lastName\": \"Smith\"}" | kafka-console-producer --topic first-flink-data --bootstrap-server kafka:9092
```

### Consume from Kafka Topic
From inside the Kafka container
```shell
kafka-console-consumer --topic first-flink-uppercase --bootstrap-server kafka:9092 --from-beginning
```

### Stop the Kafka Container
```shell
docker-compose down
```