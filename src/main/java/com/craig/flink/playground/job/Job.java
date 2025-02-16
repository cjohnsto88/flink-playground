package com.craig.flink.playground.job;

import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Job {


    public JobSubmissionResult execute(String bootstrapServers) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();


        KafkaSource<Received> source = KafkaSource.<Received>builder()
                                                .setBootstrapServers(bootstrapServers)
                                                .setTopics("words")
                                                .setGroupId("my-test-group")
                                                .setStartingOffsets(OffsetsInitializer.earliest())
                                                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(Received.class))
                                                .build();

//        KafkaSource<Received> secondSource = KafkaSource.<Received>builder()
//                                                .setBootstrapServers(bootstrapServers)
//                                                .setTopics("words")
//                                                .setGroupId("my-test-group")
//                                                .setStartingOffsets(OffsetsInitializer.earliest())
//                                                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(Received.class))
//                                                .build();

        DataStreamSource<Received> streamSource = streamEnv.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Stream Source"
        );

        streamSource.print();

//        DataStreamSource<Received> secondStreamSource = streamEnv.fromSource(
//                secondSource,
//                WatermarkStrategy.noWatermarks(),
//                "Second Stream Source"
//        );
//
//        SingleOutputStreamOperator<String> resultStream = streamSource.keyBy(Received::getContents)
//                                                                      .connect(secondStreamSource.keyBy(Received::getContents))
//                                                                      .flatMap(new RichCoFlatMapFunction<Received, Received, String>() {
//                                                                          private ValueState<Received> leftState;
//                                                                          private ValueState<Received> rightState;
//
//                                                                          @Override
//                                                                          public void open(Configuration parameters) throws Exception {
//                                                                              ValueStateDescriptor<Received> leftStateDescriptor = new ValueStateDescriptor<>("leftState", Received.class);
//                                                                              leftStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofDays(40L))
//                                                                                                                                 .build());
//
//                                                                              leftState = getRuntimeContext().getState(leftStateDescriptor);
//                                                                              rightState = getRuntimeContext().getState(new ValueStateDescriptor<>("rightState", Received.class));
//                                                                          }
//
//                                                                          @Override
//                                                                          public void flatMap1(Received received, Collector<String> collector) throws Exception {
//                                                                              Received right = rightState.value();
//
//                                                                              if (right != null) {
//                                                                                  collector.collect(received.getContents() + " " + right.getContents());
//                                                                              }
//
//                                                                              leftState.update(received);
//                                                                          }
//
//                                                                          @Override
//                                                                          public void flatMap2(Received received, Collector<String> collector) throws Exception {
//                                                                              Received left = leftState.value();
//
//                                                                              if (left != null) {
//                                                                                  collector.collect(left.getContents() + " " + received.getContents());
//                                                                              }
//
//                                                                              rightState.update(received);
//                                                                          }
//                                                                      })
//                                                                      .name("JoinLeftAndRight");


//        SingleOutputStreamOperator<CharacterCount> counts = streamSource.flatMap(new MyTokenizer())
//                                                                               .name("tokenizer")
//                                                                               .keyBy(value -> value.f0)
//                                                                               .countWindow(45, 1)
//                                                                               .sum(1)
//                                                                               .name("counter")
//                .map((MapFunction<Tuple2<String, Integer>, CharacterCount>) value -> {
//                    CharacterCount characterCount = new CharacterCount();
//                    characterCount.setWord(value.f0);
//                    characterCount.setCount(value.f1);
//
//                    return characterCount;
//                })
//                .name("mapToCharacterCount");


//        counts.sinkTo(sink).name("doTheSink");

        return streamEnv.execute("Kafka-Job");
    }

    public static class Received {

        private String contents;

        public String getContents() {
            return contents;
        }

        public void setContents(String contents) {
            this.contents = contents;
        }

        @Override
        public String toString() {
            return "Received{" +
                    "contents='" + contents + '\'' +
                    '}';
        }
    }
}
