package com.craig.flink.playground.job;

import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
class JobTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobTest.class);

    public static final String INPUT_TOPIC = "first-flink-data";
    public static final String OUTPUT_TOPIC = "first-flink-uppercase";

    @RegisterExtension
    static MiniClusterExtension miniClusterExtension = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberTaskManagers(1)
                    .setNumberSlotsPerTaskManager(4)
                    .build()
    );

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.4"));

    private ExecutorService jobSubmitter;

    @BeforeAll
    static void beforeAll() {
        Map<String, Object> config = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(config)) {
            adminClient.createTopics(List.of(
                    new NewTopic(INPUT_TOPIC, 2, (short) 1),
                    new NewTopic(OUTPUT_TOPIC, 2, (short) 1)
            ));
        }
    }

    @BeforeEach
    void setUp() {
        jobSubmitter = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void tearDown() {
        jobSubmitter.shutdownNow();
    }

    @Test
    void jobConvertsStringToUppercase(@InjectClusterClient MiniClusterClient miniClusterClient) {
        Map<String, Object> producerProperties = KafkaTestUtils.producerProps(kafka.getBootstrapServers());

        try (Producer<String, String> producer = new KafkaProducer<>(producerProperties, new StringSerializer(), new StringSerializer())) {
            // language=json
            String json = """
                    {
                      "firstName": "Craig",
                      "lastName": "Johnston"
                    }
                    """;

            producer.send(new ProducerRecord<>(INPUT_TOPIC, "{\"key\": \"ONE\"}", json));
        }

        jobSubmitter.submit(() -> {
            Job job = new Job();

            return job.execute(kafka.getBootstrapServers());
        });

        Map<String, Object> consumerProperties = KafkaTestUtils.consumerProps(kafka.getBootstrapServers(), "test-group", "true");

        // language=json
        String expectedJson = """
                {
                  "firstName": "CRAIG",
                  "lastName": "JOHNSTON"
                }
                """;

        List<ConsumerRecord<String, String>> receivedRecords = new ArrayList<>();
        try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerProperties, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(OUTPUT_TOPIC));

            await().atMost(Duration.ofSeconds(20L))
                   .untilAsserted(() -> {
                       try {
                           ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(2L));
                           records.forEach(receivedRecords::add);

                           LOGGER.info("receivedRecords: {}", receivedRecords);

                           assertThat(receivedRecords).allSatisfy(record -> {
                               LOGGER.info("Record: {}", record);

                               JSONAssert.assertEquals(expectedJson, record.value(), JSONCompareMode.STRICT);
                           });
                       } catch (IllegalStateException e) {
                           // ignore
                       } catch (Exception e) {
                           LOGGER.error("Failure when asserting", e);

                           throw e;
                       }

                       try {
                           Collection<JobStatusMessage> jobStatusMessages = miniClusterClient.listJobs().get();

                           jobStatusMessages.forEach(message -> LOGGER.info("Name: {}, State: {}", message.getJobName(), message.getJobState()));
                       } catch (InterruptedException | ExecutionException e) {
                           throw e;
                       }
                   });
        }
    }

    @Test
    void jobConvertsStringToUppercase2() {
        Map<String, Object> producerProperties = KafkaTestUtils.producerProps(kafka.getBootstrapServers());

        try (Producer<String, String> producer = new KafkaProducer<>(producerProperties, new StringSerializer(), new StringSerializer())) {
            // language=json
            String json = """
                    {
                      "firstName": "Craig",
                      "lastName": "Johnston"
                    }
                    """;

            producer.send(new ProducerRecord<>(INPUT_TOPIC, "{\"key\": \"ONE\"}", json));
        }

        jobSubmitter.submit(() -> {
            Job job = new Job();

            return job.execute(kafka.getBootstrapServers());
        });

        Map<String, Object> consumerProperties = KafkaTestUtils.consumerProps(kafka.getBootstrapServers(), "test-group", "true");

        // language=json
        String expectedJson = """
                {
                  "firstName": "CRAIG",
                  "lastName": "JOHNSTON"
                }
                """;

        List<ConsumerRecord<String, String>> receivedRecords = new ArrayList<>();
        try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerProperties, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(OUTPUT_TOPIC));

            await().atMost(Duration.ofSeconds(20L))
                   .untilAsserted(() -> {
                       try {
                           ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(2L));
                           records.forEach(receivedRecords::add);

                           LOGGER.info("receivedRecords: {}", receivedRecords);

                           assertThat(receivedRecords).allSatisfy(record -> {
                               LOGGER.info("Record: {}", record);

                               JSONAssert.assertEquals(expectedJson, record.value(), JSONCompareMode.STRICT);
                           });
                       } catch (Exception e) {
                           LOGGER.error("Failure when asserting", e);

                           throw e;
                       }
                   });
        }
    }
}