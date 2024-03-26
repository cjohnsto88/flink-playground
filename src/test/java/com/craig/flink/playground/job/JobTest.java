package com.craig.flink.playground.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.kafka.test.assertj.KafkaConditions.value;

@Testcontainers
@ExtendWith(MiniClusterExtension.class)
class JobTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(Job.class);

    public static final String INPUT_TOPIC = "first-flink-data";
    public static final String OUTPUT_TOPIC = "first-flink-uppercase";

    @RegisterExtension
    static MiniClusterExtension miniClusterExtension = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberTaskManagers(1)
                    .setNumberSlotsPerTaskManager(30)
                    .build()
    );

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.4"))
            .withLogConsumer(new Slf4jLogConsumer(LOGGER));

    @BeforeAll
    static void beforeAll() {
        Map<String, Object> config = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(config)) {
            adminClient.createTopics(List.of(
                    new NewTopic(INPUT_TOPIC, 4, (short) 1),
                    new NewTopic(OUTPUT_TOPIC, 4, (short) 1)
            ));
        }
    }

    @Test
    void containerHasBootstrapServersValue() {
        Map<String, Object> producerProperties = KafkaTestUtils.producerProps(kafka.getBootstrapServers());

        try (Producer<String, String> producer = new KafkaProducer<>(producerProperties, new StringSerializer(), new StringSerializer())) {
            producer.send(new ProducerRecord<>(INPUT_TOPIC, "key", "Hello 1"));
            producer.send(new ProducerRecord<>(INPUT_TOPIC, "key", "HeLLo 2"));
            producer.send(new ProducerRecord<>(INPUT_TOPIC, "key", "HELLO 3"));
        }

        ExecutorService executor = Executors.newSingleThreadExecutor();

        executor.submit(() -> {
            StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

            KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                                                         .setBootstrapServers(kafka.getBootstrapServers())
                                                         .setTopics(INPUT_TOPIC)
                                                         .setValueOnlyDeserializer(new SimpleStringSchema())
                                                         .setGroupId("first-flink-group")
                                                         .setStartingOffsets(OffsetsInitializer.earliest())
                                                         .build();

            KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                                                   .setBootstrapServers(kafka.getBootstrapServers())
                                                   .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                                                                                      .setTopic(OUTPUT_TOPIC)
                                                                                                      .setValueSerializationSchema(new SimpleStringSchema())
                                                                                                      .build()
                                                   )
                                                   .build();

            DataStreamSource<String> firstFlinkData = streamEnv.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "firstFlinkData");

            DataStream<String> upperCaseStream = firstFlinkData.map(input -> input.toUpperCase(Locale.ROOT));

            upperCaseStream.sinkTo(kafkaSink);

            upperCaseStream.print();

            return streamEnv.execute();
        });

        Map<String, Object> consumerProperties = KafkaTestUtils.consumerProps(kafka.getBootstrapServers(), "test-group", "true");

        List<ConsumerRecord<String, String>> receivedRecords = new ArrayList<>();
        try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerProperties, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(OUTPUT_TOPIC));

            await().atMost(Duration.ofSeconds(20L))
                   .untilAsserted(() -> {
                       ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer);
                       records.forEach(receivedRecords::add);

                       assertThat(receivedRecords).satisfiesExactlyInAnyOrder(
                               record -> assertThat(record).has(value("HELLO 1")),
                               record -> assertThat(record).has(value("HELLO 2")),
                               record -> assertThat(record).has(value("HELLO 3"))
                       );
                   });
        }
    }
}