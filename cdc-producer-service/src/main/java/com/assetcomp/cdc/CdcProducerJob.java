package com.assetcomp.cdc;

import com.assetcomp.cdc.model.AssetChangeEvent;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class CdcProducerJob {

    private static final Logger LOG = LoggerFactory.getLogger(CdcProducerJob.class);
    private static final String PROPS_FILE = "application.properties";

    public static void main(String[] args) throws Exception {
        LOG.info("Starting CDC Producer Job...");

        // 1. 스트림 실행 환경 설정
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 설정 파일 로드
        ParameterTool params = loadProperties();
        LOG.info("Properties loaded successfully.");

        // 3. Flink CDC Source 설정
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(params.get("mysql.hostname"))
                .port(params.getInt("mysql.port"))
                .databaseList(params.get("mysql.database.name"))
                .tableList(params.get("mysql.database.name") + "." + params.get("mysql.table.name"))
                .username(params.get("mysql.username"))
                .password(params.get("mysql.password"))
                .deserializer(new JsonDebeziumDeserializationSchema()) // Debezium JSON 형식으로 직렬화
                .build();

        // 4. Source로부터 데이터 스트림 생성
        DataStream<String> cdcStream = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .uid("mysql-source");

        // 5. Debezium JSON을 AssetChangeEvent 모델 기반의 JSON으로 변환
        DataStream<String> transformedStream = cdcStream.map(new AssetChangeMapper())
                .uid("asset-change-mapper");

        // 6. Kafka Sink 설정
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", params.get("kafka.bootstrap.servers"));

        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>(
                params.get("kafka.topic"),
                (KafkaSerializationSchema<String>) (element, timestamp) ->
                        new ProducerRecord<>(params.get("kafka.topic"), element.getBytes(StandardCharsets.UTF_8)),
                kafkaProps,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );

        // 7. 스트림에 Sink 추가
        transformedStream.addSink(kafkaSink)
                .name("Kafka Sink")
                .uid("kafka-sink");

        // 8. 작업 실행
        LOG.info("Executing Flink job: {}", params.get("flink.job.name"));
        env.execute(params.get("flink.job.name"));
    }

    /**
     * resources 폴더의 application.properties 파일을 로드합니다.
     */
    private static ParameterTool loadProperties() throws Exception {
        InputStream is = CdcProducerJob.class.getClassLoader().getResourceAsStream(PROPS_FILE);
        if (is == null) {
            LOG.error("Could not find properties file: {}", PROPS_FILE);
            throw new RuntimeException("Could not find " + PROPS_FILE);
        }
        return ParameterTool.fromPropertiesFile(is);
    }

    /**
     * Debezium CDC 이벤트(JSON)를 우리가 정의한 AssetChangeEvent 모델 형식의 JSON 문자열로 변환하는 MapFunction입니다.
     */
    public static class AssetChangeMapper implements MapFunction<String, String> {
        private transient ObjectMapper objectMapper;
        private static final Logger MAPPER_LOG = LoggerFactory.getLogger(AssetChangeMapper.class);


        @Override
        public String map(String value) throws Exception {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }

            try {
                ObjectNode node = objectMapper.readValue(value, ObjectNode.class);
                AssetChangeEvent event = new AssetChangeEvent();

                String op = node.get("op").asText();
                event.setOp(op);
                event.setTs_ms(node.get("ts_ms").asLong());

                ObjectNode dataNode;
                if ("d".equals(op)) { // 삭제 이벤트의 경우 'before' 필드 사용
                    dataNode = (ObjectNode) node.get("before");
                } else { // 생성 또는 수정 이벤트의 경우 'after' 필드 사용
                    dataNode = (ObjectNode) node.get("after");
                }

                event.setId(dataNode.get("id").asInt());
                event.setOwnerId(dataNode.get("owner_id").asInt()); // 'd' 포함 모든 이벤트에 owner_id 설정

                // 삭제 이벤트가 아닐 경우에만 자산 정보를 설정
                if (!"d".equals(op)) {
                    event.setAssetInfo(dataNode.get("asset_info").asText());
                }

                return objectMapper.writeValueAsString(event);
            } catch (Exception e) {
                MAPPER_LOG.error("Failed to map CDC event: {}", value, e);
                return null; // 또는 에러를 전파하거나 DLQ로 보낼 수 있음
            }
        }
    }
}