package com.assetcomp.scd2;

import com.assetcomp.scd2.model.AssetChangeEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;

import com.assetcomp.scd2.sink.StarRocksScd2Sink;

public class Scd2ConsumerJob {

    private static final Logger LOG = LoggerFactory.getLogger(Scd2ConsumerJob.class);
    private static final String PROPS_FILE = "application.properties";

    public static void main(String[] args) throws Exception {
        LOG.info("Starting SCD2 Consumer Job...");

        // 1. 스트림 실행 환경 설정
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 설정 파일 로드
        ParameterTool params = loadProperties();
        LOG.info("Properties loaded successfully.");

        // 3. Kafka Source 설정
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(params.get("kafka.bootstrap.servers"))
                .setTopics(params.get("kafka.topic"))
                .setGroupId("scd2-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 4. Source로부터 데이터 스트림 생성
        DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .uid("kafka-source");

        // 5. JSON 문자열을 BankChangeEvent 객체로 변환 (Subtask 2.2)
        DataStream<AssetChangeEvent> eventStream = kafkaStream.map(new AssetChangeEventDeserializer()).uid("deserializer");

        // 6. SCD Type 2 로직을 처리하는 커스텀 싱크에 데이터 전송 (Subtask 2.3)
        eventStream.addSink(new StarRocksScd2Sink(params)).name("StarRocks SCD2 Sink").uid("starrocks-sink");

        // 7. 작업 실행
        LOG.info("Executing Flink job: {}", params.get("flink.job.name"));
        env.execute(params.get("flink.job.name"));
    }

    /**
     * resources 폴더의 application.properties 파일을 로드합니다.
     */
    private static ParameterTool loadProperties() throws Exception {
        InputStream is = Scd2ConsumerJob.class.getClassLoader().getResourceAsStream(PROPS_FILE);
        if (is == null) {
            LOG.error("Could not find properties file: {}", PROPS_FILE);
            throw new RuntimeException("Could not find " + PROPS_FILE);
        }
        return ParameterTool.fromPropertiesFile(is);
    }

    public static class AssetChangeEventDeserializer implements MapFunction<String, AssetChangeEvent> {
        private transient ObjectMapper objectMapper;

        @Override
        public AssetChangeEvent map(String value) throws Exception {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }
            return objectMapper.readValue(value, AssetChangeEvent.class);
        }
    }
}
