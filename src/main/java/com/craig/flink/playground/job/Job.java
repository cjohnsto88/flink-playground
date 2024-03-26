package com.craig.flink.playground.job;

import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Locale;

public class Job {
    public JobSubmissionResult execute(String bootstrapServers) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                                                     .setBootstrapServers(bootstrapServers)
                                                     .setTopics("first-flink-data")
                                                     .setValueOnlyDeserializer(new SimpleStringSchema())
                                                     .setGroupId("first-flink-group")
                                                     .setStartingOffsets(OffsetsInitializer.earliest())
                                                     .build();

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                                               .setBootstrapServers(bootstrapServers)
                                               .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                                                                                  .setTopic("first-flink-uppercase")
                                                                                                  .setValueSerializationSchema(new SimpleStringSchema())
                                                                                                  .build()
                                               )
//                                               .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                                               .build();

        DataStreamSource<String> firstFlinkData = streamEnv.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "firstFlinkData");

        DataStream<String> upperCaseStream = firstFlinkData.map(input -> input.toUpperCase(Locale.ROOT));

        upperCaseStream.sinkTo(kafkaSink);
        return streamEnv.execute();
    }
}
