package com.craig.flink.playground.job;

import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Job {
    public JobSubmissionResult execute(String bootstrapServers) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);

        String createInputTableSql = """
                CREATE TABLE KafkaInput (
                  `firstName` VARCHAR,
                  `lastName` VARCHAR
                ) WITH (
                  'connector' = 'kafka',
                  'topic' = 'first-flink-data',
                  'properties.bootstrap.servers' = '%s',
                  'properties.group.id' = 'first-flink-group',
                  'scan.startup.mode' = 'earliest-offset',
                  'format' = 'json'
                )
                """.formatted(bootstrapServers);

        tableEnv.executeSql(createInputTableSql);

        tableEnv.executeSql("""
                CREATE TABLE KafkaOutput (
                  `firstName` VARCHAR,
                  `lastName` VARCHAR
                ) WITH (
                  'connector' = 'kafka',
                  'topic' = 'first-flink-uppercase',
                  'properties.bootstrap.servers' = '%s',
                  'value.format' = 'json'
                )
                """.formatted(bootstrapServers));

        tableEnv.executeSql("""
                CREATE TABLE PrintOutput (
                  `firstName` VARCHAR,
                  `lastName` VARCHAR
                ) WITH (
                  'connector' = 'print',
                  'print-identifier' = 'To-Upper: '
                )
                """);

        Table firstNameUpperTable = tableEnv.sqlQuery("""
                SELECT UPPER(firstName), UPPER(lastName)
                FROM KafkaInput
                """);

        firstNameUpperTable.insertInto("KafkaOutput").execute();
        firstNameUpperTable.insertInto("PrintOutput").execute();

        return null;
    }
}
