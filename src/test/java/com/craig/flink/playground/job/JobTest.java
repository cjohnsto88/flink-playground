package com.craig.flink.playground.job;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class JobTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(Job.class);

    @Container
    KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
            .withLogConsumer(new Slf4jLogConsumer(LOGGER));


    @BeforeEach
    void setUp() {
        LOGGER.info("Hello");

        System.out.println("setup");
    }

    @Test
    void containerHasBootstrapServersValue() {
        String bootstrapServers = kafka.getBootstrapServers();

        assertThat(bootstrapServers).isNotEmpty();
    }
}